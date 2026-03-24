"""Product matching engine with Standard and Hardcore Pris Prioritet modes.

Matches input products (e.g., from kommune tenders) against the OneMed/Jeeves
catalog to find the best matching product. Two modes:

- Standard: Best semantic match wins, price is secondary.
- Hardcore Pris Prioritet: Cheapest sufficiently relevant candidate wins.
  "Billigste forsvarlige match" — not "perfekt match uansett pris".

Pipeline:
1. Text-based candidate retrieval (TF-IDF / keyword overlap)
2. Candidate scoring (relevance)
3. AI verification (optional, mode-aware)
4. ALC price ranking (dominant in Hardcore Pris Prioritet)
5. Final selection
"""

import json
import logging
import os
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


# ── Match Mode ──

class MatchMode(str, Enum):
    STANDARD = "standard"
    HARDCORE_PRICE = "hardcore_price"
    OWN_BRAND = "own_brand"
    STRICT_QUALITY = "strict_quality"


# ── Data structures ──

@dataclass
class MatchCandidate:
    """A candidate product from the catalog."""
    article_number: str
    product_name: str = ""
    specification: str = ""
    supplier: str = ""
    product_brand: str = ""
    alc_price: Optional[float] = None  # ALC price if available
    relevance_score: float = 0.0  # 0-100
    text_similarity: float = 0.0  # Raw text similarity score
    category_match: bool = True  # Whether product type/category matches
    hard_mismatch: bool = False  # Whether this is an obvious mismatch
    mismatch_reason: str = ""
    ai_relevance_score: Optional[float] = None  # AI-assigned relevance (0-100)
    ai_explanation: str = ""
    final_score: float = 0.0  # Combined final score used for ranking
    is_sufficiently_relevant: bool = False  # Passes the "godt nok" threshold
    rank: int = 0
    tags: list = field(default_factory=list)  # e.g. ["billigst", "best_match"]

    def to_dict(self) -> dict:
        return {
            "article_number": self.article_number,
            "product_name": self.product_name,
            "specification": self.specification,
            "supplier": self.supplier,
            "product_brand": self.product_brand,
            "alc_price": self.alc_price,
            "alc_price_display": f"kr {self.alc_price:,.2f}".replace(",", " ") if self.alc_price is not None else "Mangler",
            "relevance_score": round(self.relevance_score, 1),
            "text_similarity": round(self.text_similarity, 1),
            "category_match": self.category_match,
            "hard_mismatch": self.hard_mismatch,
            "mismatch_reason": self.mismatch_reason,
            "ai_relevance_score": round(self.ai_relevance_score, 1) if self.ai_relevance_score is not None else None,
            "ai_explanation": self.ai_explanation,
            "final_score": round(self.final_score, 1),
            "is_sufficiently_relevant": self.is_sufficiently_relevant,
            "rank": self.rank,
            "tags": self.tags,
        }


@dataclass
class MatchResult:
    """Result of matching one input product."""
    input_row: int  # Row number in input file
    input_product_name: str = ""
    input_specification: str = ""
    input_article_number: str = ""  # Input artnr if provided
    match_mode: str = MatchMode.STANDARD.value
    candidates: list = field(default_factory=list)  # List of MatchCandidate
    selected_candidate: Optional[str] = None  # Article number of selected candidate
    selected_by: str = "auto"  # "auto" or "manual"
    status: str = "pending"  # pending, matched, no_match, multiple, manual_review
    comment: str = ""

    def get_selected(self) -> Optional[MatchCandidate]:
        """Get the selected candidate."""
        if self.selected_candidate:
            for c in self.candidates:
                if c.article_number == self.selected_candidate:
                    return c
        return None

    def to_dict(self) -> dict:
        selected = self.get_selected()
        return {
            "input_row": self.input_row,
            "input_product_name": self.input_product_name,
            "input_specification": self.input_specification,
            "input_article_number": self.input_article_number,
            "match_mode": self.match_mode,
            "candidates": [c.to_dict() for c in self.candidates],
            "candidate_count": len(self.candidates),
            "selected_candidate": self.selected_candidate,
            "selected_by": self.selected_by,
            "selected_product_name": selected.product_name if selected else None,
            "selected_alc_price": selected.alc_price if selected else None,
            "selected_relevance_score": round(selected.relevance_score, 1) if selected else None,
            "status": self.status,
            "comment": self.comment,
        }


# ── Configuration ──

# Standard mode: relevance is king
STANDARD_MIN_RELEVANCE = 65  # Minimum relevance score to be considered
STANDARD_TOP_CANDIDATES = 5  # Show top N candidates

# Hardcore Pris Prioritet: price is king among "good enough" candidates
HARDCORE_MIN_RELEVANCE = 50  # Lower threshold — more candidates pass
HARDCORE_SUFFICIENT_RELEVANCE = 55  # "Tilstrekkelig relevant" threshold
HARDCORE_TOP_CANDIDATES = 8  # Show more candidates for user choice

# Own Brand (Egne merkevarer): prefer OneMed/house brands
OWN_BRAND_MIN_RELEVANCE = 60
OWN_BRAND_TOP_CANDIDATES = 6
OWN_BRAND_BRANDS = {"onemed", "selefa", "mediline", "abena"}  # Known house/own brands

# Strict Quality: highest relevance threshold, only precise matches
STRICT_QUALITY_MIN_RELEVANCE = 80
STRICT_QUALITY_TOP_CANDIDATES = 3

# Hard mismatch detection keywords — product types that must NOT be swapped
PRODUCT_TYPE_GROUPS = [
    {"keywords": ["hansker", "hanske", "gloves", "glove"], "type": "hansker"},
    {"keywords": ["bandasje", "bandage", "plaster"], "type": "bandasje"},
    {"keywords": ["sprøyte", "syringe", "kanyle", "needle"], "type": "injeksjon"},
    {"keywords": ["kateter", "catheter"], "type": "kateter"},
    {"keywords": ["maske", "munnbind", "mask"], "type": "maske"},
    {"keywords": ["desinfeksjon", "desinfisering", "antiseptisk", "disinfect"], "type": "desinfeksjon"},
    {"keywords": ["sonde", "tube", "ernæring"], "type": "sonde"},
    {"keywords": ["stomi", "stoma", "pose"], "type": "stomi"},
    {"keywords": ["sår", "wound", "kompresse", "kompress"], "type": "sårbehandling"},
    {"keywords": ["inkontinens", "bleie", "bind", "absorbent"], "type": "inkontinens"},
    {"keywords": ["sko", "trekk", "skoovertrekk", "shoe"], "type": "skotrekk"},
    {"keywords": ["hårnett", "lue", "hodeplagg", "cap", "hair"], "type": "hodeplagg"},
    {"keywords": ["forkle", "smekke", "apron"], "type": "forkle"},
    {"keywords": ["sengetøy", "laken", "pute", "bedding"], "type": "sengetøy"},
]


def _detect_product_type(text: str) -> Optional[str]:
    """Detect product type from text using keyword matching."""
    if not text:
        return None
    text_lower = text.lower()
    for group in PRODUCT_TYPE_GROUPS:
        for kw in group["keywords"]:
            if kw in text_lower:
                return group["type"]
    return None


def _is_hard_mismatch(input_text: str, candidate_text: str) -> tuple[bool, str]:
    """Check for hard mismatches — different product types.

    Returns (is_mismatch, reason).
    """
    input_type = _detect_product_type(input_text)
    candidate_type = _detect_product_type(candidate_text)

    if input_type and candidate_type and input_type != candidate_type:
        return True, f"Produkttype-mismatch: input er '{input_type}', kandidat er '{candidate_type}'"

    return False, ""


# ── Text similarity ──

def _tokenize(text: str) -> set:
    """Simple tokenization for text similarity."""
    if not text:
        return set()
    # Lowercase, remove punctuation, split
    text = text.lower()
    text = re.sub(r'[^\w\sæøåÆØÅ]', ' ', text)
    tokens = set(text.split())
    # Remove very short tokens
    tokens = {t for t in tokens if len(t) > 1}
    return tokens


def _text_similarity(text_a: str, text_b: str) -> float:
    """Compute Jaccard-like similarity between two texts. Returns 0-100."""
    tokens_a = _tokenize(text_a)
    tokens_b = _tokenize(text_b)

    if not tokens_a or not tokens_b:
        return 0.0

    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b

    if not union:
        return 0.0

    jaccard = len(intersection) / len(union)

    # Boost for key term overlap (longer shared tokens are more significant)
    key_overlap = sum(1 for t in intersection if len(t) >= 4)
    key_total = sum(1 for t in tokens_a if len(t) >= 4)

    if key_total > 0:
        key_ratio = key_overlap / key_total
        # Weighted: 60% Jaccard, 40% key term overlap
        score = (jaccard * 0.6 + key_ratio * 0.4) * 100
    else:
        score = jaccard * 100

    return min(100.0, score)


def _combined_text(name: str, spec: str) -> str:
    """Combine product name and specification for matching."""
    parts = []
    if name:
        parts.append(name.strip())
    if spec:
        parts.append(spec.strip())
    return " ".join(parts)


# ── ALC Price handling ──

def parse_alc_price(value) -> Optional[float]:
    """Parse an ALC price value from various formats."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) if value > 0 else None
    if isinstance(value, str):
        # Remove currency symbols, spaces, and handle comma decimals
        cleaned = value.strip().replace("kr", "").replace("NOK", "").replace(" ", "")
        cleaned = cleaned.replace(",", ".")
        try:
            val = float(cleaned)
            return val if val > 0 else None
        except (ValueError, TypeError):
            return None
    return None


# ── Candidate generation ──

def generate_candidates(
    input_name: str,
    input_spec: str,
    catalog: list[dict],
    match_mode: str = MatchMode.STANDARD.value,
    max_candidates: int = 20,
) -> list[MatchCandidate]:
    """Generate and score match candidates from the catalog.

    Args:
        input_name: Input product name to match
        input_spec: Input product specification
        catalog: List of dicts with catalog products (article_number, item_description, specification, supplier, product_brand, alc_price)
        match_mode: "standard" or "hardcore_price"
        max_candidates: Maximum candidates to return

    Returns:
        List of MatchCandidate sorted by final_score descending
    """
    is_hardcore = match_mode == MatchMode.HARDCORE_PRICE.value
    min_relevance = {
        MatchMode.HARDCORE_PRICE.value: HARDCORE_MIN_RELEVANCE,
        MatchMode.OWN_BRAND.value: OWN_BRAND_MIN_RELEVANCE,
        MatchMode.STRICT_QUALITY.value: STRICT_QUALITY_MIN_RELEVANCE,
    }.get(match_mode, STANDARD_MIN_RELEVANCE)
    sufficient_threshold = HARDCORE_SUFFICIENT_RELEVANCE if is_hardcore else min_relevance

    input_text = _combined_text(input_name, input_spec)
    if not input_text.strip():
        return []

    candidates = []

    for item in catalog:
        artnr = item.get("article_number", "")
        name = item.get("item_description", "") or item.get("product_name", "") or ""
        spec = item.get("specification", "") or ""
        supplier = item.get("supplier", "") or ""
        brand = item.get("product_brand", "") or ""
        alc = parse_alc_price(item.get("alc_price"))

        candidate_text = _combined_text(name, spec)
        if not candidate_text.strip():
            continue

        # Text similarity
        similarity = _text_similarity(input_text, candidate_text)

        # Early cutoff — skip very low similarity
        if similarity < min_relevance * 0.5:
            continue

        # Hard mismatch check
        is_mismatch, mismatch_reason = _is_hard_mismatch(input_text, candidate_text)

        # Build candidate
        candidate = MatchCandidate(
            article_number=artnr,
            product_name=name,
            specification=spec,
            supplier=supplier,
            product_brand=brand,
            alc_price=alc,
            text_similarity=similarity,
            relevance_score=similarity,  # Will be refined by AI if available
            hard_mismatch=is_mismatch,
            mismatch_reason=mismatch_reason,
            category_match=not is_mismatch,
        )

        # Mark as sufficiently relevant
        if not is_mismatch and similarity >= sufficient_threshold:
            candidate.is_sufficiently_relevant = True

        candidates.append(candidate)

    # Sort by relevance descending, take top N for further processing
    candidates.sort(key=lambda c: c.relevance_score, reverse=True)
    candidates = candidates[:max_candidates]

    # Compute final scores
    _compute_final_scores(candidates, match_mode)

    # Tag candidates
    _tag_candidates(candidates, match_mode)

    # Assign ranks
    for i, c in enumerate(candidates):
        c.rank = i + 1

    return candidates


def _compute_final_scores(candidates: list[MatchCandidate], match_mode: str) -> None:
    """Compute final scores based on match mode.

    Standard mode: final_score = relevance_score (price is tie-breaker only)
    Hardcore Pris Prioritet: final_score heavily weights price among sufficiently relevant candidates
    Own Brand: boosts own/house brand candidates
    Strict Quality: only allows high-relevance matches
    """
    if not candidates:
        return

    is_hardcore = match_mode == MatchMode.HARDCORE_PRICE.value
    is_own_brand = match_mode == MatchMode.OWN_BRAND.value
    is_strict = match_mode == MatchMode.STRICT_QUALITY.value

    if is_hardcore:
        # Hardcore Pris Prioritet mode — price dominates
        prices = [c.alc_price for c in candidates if c.alc_price is not None and not c.hard_mismatch]
        if prices:
            min_price = min(prices)
            max_price = max(prices)
            price_range = max_price - min_price if max_price > min_price else 1.0
        else:
            min_price = 0
            price_range = 1.0

        for c in candidates:
            if c.hard_mismatch:
                c.final_score = 0.0
                continue
            if not c.is_sufficiently_relevant:
                c.final_score = c.relevance_score * 0.3
                continue
            relevance_component = c.relevance_score
            if c.alc_price is not None and prices:
                price_score = (1.0 - (c.alc_price - min_price) / price_range) * 100 if price_range > 0 else 100.0
                c.final_score = relevance_component * 0.30 + price_score * 0.70
            else:
                c.final_score = relevance_component * 0.50

    elif is_own_brand:
        # Own Brand mode — boost own/house brands
        for c in candidates:
            if c.hard_mismatch:
                c.final_score = 0.0
                continue
            base = c.relevance_score
            # Boost if supplier or brand matches known own brands
            brand_text = (c.supplier + " " + c.product_brand).lower()
            is_own = any(b in brand_text for b in OWN_BRAND_BRANDS)
            if is_own:
                c.final_score = base * 1.0 + 15.0  # +15 point bonus for own brands
            else:
                c.final_score = base * 0.85  # 15% penalty for external brands

    elif is_strict:
        # Strict Quality mode — only high-relevance matches
        for c in candidates:
            if c.hard_mismatch:
                c.final_score = 0.0
            elif c.relevance_score < STRICT_QUALITY_MIN_RELEVANCE:
                c.final_score = c.relevance_score * 0.3  # Heavy penalty below threshold
            else:
                c.final_score = c.relevance_score

    else:
        # Standard mode: relevance drives ranking
        for c in candidates:
            if c.hard_mismatch:
                c.final_score = 0.0
            else:
                c.final_score = c.relevance_score

    candidates.sort(key=lambda c: c.final_score, reverse=True)


def _tag_candidates(candidates: list[MatchCandidate], match_mode: str) -> None:
    """Add descriptive tags to candidates."""
    if not candidates:
        return

    is_hardcore = match_mode == MatchMode.HARDCORE_PRICE.value
    is_own_brand = match_mode == MatchMode.OWN_BRAND.value

    # Find best match (highest relevance among non-mismatches)
    valid = [c for c in candidates if not c.hard_mismatch]
    if valid:
        best_relevance = max(valid, key=lambda c: c.relevance_score)
        best_relevance.tags.append("best_match")

    # Find cheapest among sufficiently relevant
    priced_relevant = [c for c in candidates if c.is_sufficiently_relevant and c.alc_price is not None and not c.hard_mismatch]
    if priced_relevant:
        cheapest = min(priced_relevant, key=lambda c: c.alc_price)
        cheapest.tags.append("billigst")
        if is_hardcore:
            cheapest.tags.append("anbefalt_hardcore")

    # Tag own brand candidates
    if is_own_brand:
        for c in valid:
            brand_text = (c.supplier + " " + c.product_brand).lower()
            if any(b in brand_text for b in OWN_BRAND_BRANDS):
                c.tags.append("eget_merke")


# ── AI-assisted relevance verification ──

AI_MATCH_MODEL = os.environ.get("AI_MATCH_MODEL", "claude-haiku-4-5")

MATCH_VERIFY_STANDARD_PROMPT = """Du er en ekspert på produktmatching for medisinske produkter og helseprodukter.

Vurder om kandidatproduktet er en god match for innproduktet.

Score kandidaten 0-100:
- 90-100: Nesten identisk produkt (samme type, spesifikasjon, funksjon)
- 70-89: Godt alternativ (samme type, lignende spesifikasjon)
- 50-69: Mulig alternativ (samme hovedtype, avvikende spesifikasjon)
- 30-49: Tvilsomt (lignende, men vesentlige forskjeller)
- 0-29: Feil produkt (ulik type eller funksjon)

Vær streng — krev høy likhet for høy score.

Svar med JSON:
{{"relevance_score": <0-100>, "explanation": "<kort forklaring på norsk>"}}"""

MATCH_VERIFY_HARDCORE_PROMPT = """Du er en ekspert på produktmatching for medisinske produkter og helseprodukter.

MODUS: Hardcore Pris Prioritet
Målet er å finne det billigste FORSVARLIGE alternativet — ikke den perfekte matchen.

Vurder om kandidatproduktet er tilstrekkelig relevant som alternativ til innproduktet.
Vær MER tolerant enn normalt for mindre avvik, men stopp åpenbare feiltreff.

Score kandidaten 0-100:
- 70-100: Klart relevant alternativ (riktig type, akseptabel variasjon)
- 50-69: Akseptabelt alternativ (riktig hovedtype, noe avvik i detaljer)
- 30-49: Grensetilfelle (kan forsvares, men betydelige forskjeller)
- 0-29: Feil produkt (ulik type, funksjon eller bruksområde — avvis)

Viktig:
- Riktig produkttype MÅ stemme (f.eks. hansker → hansker)
- Mindre avvik i spesifikasjon (materiale, størrelse) er OK
- Ulik produkttype = automatisk lav score uansett pris
- Formålet er prisoptimalisering, IKKE å foreslå feil produkt

Svar med JSON:
{{"relevance_score": <0-100>, "explanation": "<kort forklaring på norsk>"}}"""

MATCH_VERIFY_USER_TEMPLATE = """Innprodukt:
Navn: {input_name}
Spesifikasjon: {input_spec}

Kandidatprodukt:
Art.nr: {candidate_artnr}
Navn: {candidate_name}
Spesifikasjon: {candidate_spec}
Produsent: {candidate_supplier}
ALC-pris: {candidate_price}

Er kandidaten en relevant match?"""


async def ai_verify_candidates(
    input_name: str,
    input_spec: str,
    candidates: list[MatchCandidate],
    match_mode: str = MatchMode.STANDARD.value,
    max_verify: int = 10,
) -> list[MatchCandidate]:
    """Use AI to verify/re-score candidate relevance.

    Returns updated candidates with AI scores.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.info("No ANTHROPIC_API_KEY — skipping AI match verification")
        return candidates

    try:
        import anthropic
        client = anthropic.AsyncAnthropic(api_key=api_key)
    except ImportError:
        logger.warning("anthropic package not available — skipping AI verification")
        return candidates

    is_hardcore = match_mode == MatchMode.HARDCORE_PRICE.value
    system_prompt = MATCH_VERIFY_HARDCORE_PROMPT if is_hardcore else MATCH_VERIFY_STANDARD_PROMPT

    # Only verify top candidates (already pre-filtered)
    to_verify = [c for c in candidates if not c.hard_mismatch][:max_verify]

    for candidate in to_verify:
        try:
            price_str = f"kr {candidate.alc_price:,.2f}" if candidate.alc_price else "Ikke oppgitt"
            user_prompt = MATCH_VERIFY_USER_TEMPLATE.format(
                input_name=input_name or "(mangler)",
                input_spec=input_spec or "(mangler)",
                candidate_artnr=candidate.article_number,
                candidate_name=candidate.product_name or "(mangler)",
                candidate_spec=candidate.specification or "(mangler)",
                candidate_supplier=candidate.supplier or "(mangler)",
                candidate_price=price_str,
            )

            response = await client.messages.create(
                model=AI_MATCH_MODEL,
                max_tokens=500,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            )

            text = next((b.text for b in response.content if b.type == "text"), "")
            # Parse JSON from response
            text = text.strip()
            try:
                result = json.loads(text)
            except json.JSONDecodeError:
                # Try to extract JSON
                start = text.find("{")
                end = text.rfind("}") + 1
                if start >= 0 and end > start:
                    result = json.loads(text[start:end])
                else:
                    continue

            ai_score = max(0, min(100, result.get("relevance_score", 0)))
            candidate.ai_relevance_score = ai_score
            candidate.ai_explanation = result.get("explanation", "")

            # Update relevance score: blend text similarity with AI score
            # AI score is more trustworthy for relevance assessment
            candidate.relevance_score = candidate.text_similarity * 0.3 + ai_score * 0.7

            # Update sufficient relevance based on AI score
            threshold = HARDCORE_SUFFICIENT_RELEVANCE if is_hardcore else STANDARD_MIN_RELEVANCE
            candidate.is_sufficiently_relevant = (
                not candidate.hard_mismatch and candidate.relevance_score >= threshold
            )

        except Exception as e:
            logger.warning(f"AI verification failed for {candidate.article_number}: {e}")
            continue

    # Recompute final scores after AI verification
    _compute_final_scores(candidates, match_mode)
    _tag_candidates(candidates, match_mode)

    # Re-rank
    for i, c in enumerate(candidates):
        c.rank = i + 1

    return candidates


# ── Full matching pipeline ──

async def match_product(
    input_name: str,
    input_spec: str,
    catalog: list[dict],
    match_mode: str = MatchMode.STANDARD.value,
    use_ai: bool = True,
    input_row: int = 0,
    input_article_number: str = "",
) -> MatchResult:
    """Run the full matching pipeline for one input product.

    Returns a MatchResult with ranked candidates.
    """
    is_hardcore = match_mode == MatchMode.HARDCORE_PRICE.value
    top_n = {
        MatchMode.HARDCORE_PRICE.value: HARDCORE_TOP_CANDIDATES,
        MatchMode.OWN_BRAND.value: OWN_BRAND_TOP_CANDIDATES,
        MatchMode.STRICT_QUALITY.value: STRICT_QUALITY_TOP_CANDIDATES,
    }.get(match_mode, STANDARD_TOP_CANDIDATES)

    result = MatchResult(
        input_row=input_row,
        input_product_name=input_name,
        input_specification=input_spec,
        input_article_number=input_article_number,
        match_mode=match_mode,
    )

    # Step 1: Generate candidates
    candidates = generate_candidates(
        input_name, input_spec, catalog,
        match_mode=match_mode,
        max_candidates=top_n * 3,  # Get extra for AI filtering
    )

    if not candidates:
        result.status = "no_match"
        result.comment = "Ingen kandidater funnet i katalogen"
        return result

    # Step 2: AI verification (if enabled)
    if use_ai:
        candidates = await ai_verify_candidates(
            input_name, input_spec, candidates,
            match_mode=match_mode,
            max_verify=min(10, len(candidates)),
        )

    # Step 3: Filter and trim
    # Remove hard mismatches from display
    display_candidates = [c for c in candidates if not c.hard_mismatch]

    # Keep only top N
    display_candidates = display_candidates[:top_n]

    result.candidates = display_candidates

    # Step 4: Auto-select best candidate
    if display_candidates:
        best = display_candidates[0]  # Already sorted by final_score

        if best.relevance_score >= (HARDCORE_SUFFICIENT_RELEVANCE if is_hardcore else STANDARD_MIN_RELEVANCE):
            result.selected_candidate = best.article_number
            result.selected_by = "auto"
            result.status = "matched"

            if len(display_candidates) > 1 and is_hardcore:
                # In hardcore mode, flag for review if multiple good candidates
                relevant_count = sum(1 for c in display_candidates if c.is_sufficiently_relevant)
                if relevant_count > 1:
                    result.status = "multiple"
                    result.comment = f"{relevant_count} relevante alternativer funnet — anbefaler gjennomgang"
            elif len(display_candidates) > 1:
                relevant_count = sum(1 for c in display_candidates if c.relevance_score >= STANDARD_MIN_RELEVANCE)
                if relevant_count > 1:
                    result.comment = f"{relevant_count} mulige matcher — beste er valgt automatisk"
        else:
            result.status = "manual_review"
            result.comment = "Ingen kandidater møter relevanskravet — krever manuell vurdering"
    else:
        result.status = "no_match"
        result.comment = "Ingen relevante kandidater funnet"

    return result


def select_candidate(result: MatchResult, article_number: str) -> bool:
    """Manually select a candidate for a match result.

    Returns True if selection was successful.
    """
    for c in result.candidates:
        if c.article_number == article_number:
            result.selected_candidate = article_number
            result.selected_by = "manual"
            result.status = "matched"
            result.comment = f"Manuelt valgt av bruker"
            return True
    return False
