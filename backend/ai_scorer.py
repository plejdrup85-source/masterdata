"""AI-powered product quality scoring and enrichment using Claude API.

Uses Claude to evaluate product data quality and suggest improvements
for medical/healthcare products in the OneMed catalog.

Two AI roles:
1. SCORING: Evaluate current product data quality (score_product_async)
2. REVIEW: Polish and quality-check enrichment suggestions (review_suggestions_async)

The REVIEW step acts as an editorial quality gate — it does NOT invent facts,
only improves language, rejects unusable fragments, and ensures output quality.
"""

import json
import logging
import os
from typing import Optional

import anthropic

logger = logging.getLogger(__name__)

# Use Haiku for cost-effective batch scoring, configurable via env
AI_MODEL = os.environ.get("AI_SCORING_MODEL", "claude-haiku-4-5")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

SCORING_SYSTEM_PROMPT = """Du er en ekspert på produktdatakvalitet for medisinske produkter og helseprodukter.

Du evaluerer produktdata basert på disse kriteriene:

## Produktnavn (0-100)
- MÅ inneholde produkttype (f.eks. "Hansker", "Bandasje", "Hårnett")
- MÅ inneholde minst én differensierende egenskap (materiale, størrelse, farge, etc.)
- MÅ IKKE være generisk (f.eks. bare "Produkt" eller "Vare")
- Høy score: Spesifikt, klart, inneholder type + egenskap
- Lav score: Generisk, mangler type, kun tall/koder

## Beskrivelse (0-100)
- MÅ forklare bruksområde
- MÅ inkludere nøkkelegenskaper
- BØR inkludere differensiatorer fra konkurrenter
- BØR være lesbar og velformatert
- Høy score: Komplett, strukturert, forklarer bruk og fordeler
- Lav score: For kort, kopierer produktnavn, mangler bruksinformasjon

## Spesifikasjon (0-100)
- MÅ inneholde strukturerte attributter (materiale, størrelse, pakningsenhet, tekniske egenskaper)
- MÅ ha minst 2 meningsfulle spesifikasjonsfelter
- AVVIS hvis den bare gjentar beskrivelsen
- Høy score: Flere tekniske detaljer, strukturert, målbare verdier
- Lav score: Mangler, vag, kun repetisjon av beskrivelse

## Kategori (0-100)
- MÅ matche produkttype logisk
- BØR følge et hierarki (f.eks. Hansker → Nitril → Undersøkelseshansker)
- Høy score: Flernivå-hierarki, logisk match
- Lav score: Mangler, for generisk, mismatch med produkttype

## Pakningsinformasjon (0-100)
- MÅ inkludere antall per forpakning
- BØR inkludere transportforpakning hvis relevant
- Høy score: Tydelig antall per pk, transportpakning inkludert
- Lav score: Mangler, uklar

## VIKTIGE REGLER
- ALDRI hallusiner eller finn opp fakta
- Bruk KUN informasjonen som er gitt
- Vær konservativ - dette er medisinske produkter
- Svar ALLTID med gyldig JSON
- Gi konkrete, handlingsbare forbedringsforslag"""

SCORING_USER_TEMPLATE = """Evaluer kvaliteten på følgende produktdata:

Produktnavn: {product_name}
Beskrivelse: {description}
Spesifikasjon: {specification}
Kategori: {category}
Pakningsinformasjon: {packaging}

Svar med denne JSON-strukturen (INGEN annen tekst, kun JSON):
{{
  "overall_score": <0-100>,
  "field_scores": {{
    "name": <0-100>,
    "description": <0-100>,
    "specification": <0-100>,
    "category": <0-100>,
    "packaging": <0-100>
  }},
  "issues": [
    "<kort beskrivelse av problem 1>",
    "<kort beskrivelse av problem 2>"
  ],
  "improvement_suggestions": [
    "<konkret forbedringsforslag 1>",
    "<konkret forbedringsforslag 2>"
  ]
}}"""

ENRICHMENT_SYSTEM_PROMPT = """Du er en ekspert på produktdata for medisinske produkter og helseprodukter.

Din oppgave er å foreslå forbedringer til produktdata basert på det som allerede finnes.

## VIKTIGE REGLER
- ALDRI finn opp fakta eller tekniske spesifikasjoner
- Du kan BARE:
  - Skrive om beskrivelser til WEBSHOP-KLARE produkttekster (2–4 korte avsnitt, profesjonelt norsk)
  - Fjerne støy som tabelldata, artikkelnumre, metadata, og PDF-artefakter
  - Foreslå SANNSYNLIGE manglende attributter basert på produkttype (f.eks. at hansker bør ha materialeinfo)
  - Foreslå kategoristruktur basert på produktnavn
  - Forbedre pakningsinformasjon basert på tilgjengelig info
- Merk tydelig hva som er FORSLAG vs. FAKTA
- Vær konservativ - dette er medisinske produkter
- Svar ALLTID med gyldig JSON"""

ENRICHMENT_USER_TEMPLATE = """Foreslå forbedringer for følgende produktdata:

Produktnavn: {product_name}
Beskrivelse: {description}
Spesifikasjon: {specification}
Kategori: {category}
Pakningsinformasjon: {packaging}

Svar med denne JSON-strukturen (INGEN annen tekst, kun JSON):
{{
  "improved_description": "<forbedret beskrivelse eller null hvis OK>",
  "missing_specifications": [
    "<spesifikasjon som sannsynligvis mangler basert på produkttype>"
  ],
  "suggested_category": "<foreslått kategorihierarki eller null>",
  "packaging_suggestions": "<forslag til forbedring av pakningsinfo eller null>"
}}"""


def _get_client() -> Optional[anthropic.Anthropic]:
    """Get Anthropic client, returns None if no API key configured."""
    api_key = ANTHROPIC_API_KEY
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY not set, AI scoring disabled")
        return None
    return anthropic.Anthropic(api_key=api_key)


def _safe_str(val: Optional[str]) -> str:
    """Convert None to descriptive string for prompts."""
    return val if val else "(mangler)"


def _parse_json_response(text: str) -> Optional[dict]:
    """Extract and parse JSON from Claude's response."""
    # Try direct parse first
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try to find JSON in markdown code block
    if "```" in text:
        parts = text.split("```")
        for part in parts:
            cleaned = part.strip()
            if cleaned.startswith("json"):
                cleaned = cleaned[4:].strip()
            try:
                return json.loads(cleaned)
            except json.JSONDecodeError:
                continue

    # Try to find JSON object in text
    start = text.find("{")
    end = text.rfind("}") + 1
    if start >= 0 and end > start:
        try:
            return json.loads(text[start:end])
        except json.JSONDecodeError:
            pass

    return None


def score_product(
    product_name: Optional[str] = None,
    description: Optional[str] = None,
    specification: Optional[str] = None,
    category: Optional[str] = None,
    packaging: Optional[str] = None,
) -> Optional[dict]:
    """Score product data quality using Claude.

    Returns dict with overall_score, field_scores, issues, improvement_suggestions.
    Returns None if AI scoring is not available.
    """
    client = _get_client()
    if not client:
        return None

    user_prompt = SCORING_USER_TEMPLATE.format(
        product_name=_safe_str(product_name),
        description=_safe_str(description),
        specification=_safe_str(specification),
        category=_safe_str(category),
        packaging=_safe_str(packaging),
    )

    try:
        response = client.messages.create(
            model=AI_MODEL,
            max_tokens=2000,
            system=SCORING_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )

        text = next(
            (b.text for b in response.content if b.type == "text"), ""
        )
        result = _parse_json_response(text)
        if not result:
            logger.error(f"Failed to parse AI scoring response: {text[:200]}")
            return None

        # Validate structure
        if "overall_score" not in result or "field_scores" not in result:
            logger.error(f"AI scoring response missing required fields: {result}")
            return None

        # Clamp scores to 0-100
        result["overall_score"] = max(0, min(100, result["overall_score"]))
        for key in result.get("field_scores", {}):
            result["field_scores"][key] = max(
                0, min(100, result["field_scores"][key])
            )

        return result

    except anthropic.APIError as e:
        logger.error(f"Claude API error during scoring: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during AI scoring: {e}")
        return None


def enrich_product(
    product_name: Optional[str] = None,
    description: Optional[str] = None,
    specification: Optional[str] = None,
    category: Optional[str] = None,
    packaging: Optional[str] = None,
) -> Optional[dict]:
    """Get AI-powered enrichment suggestions for product data.

    Returns dict with improved_description, missing_specifications,
    suggested_category, packaging_suggestions.
    Returns None if AI enrichment is not available.
    """
    client = _get_client()
    if not client:
        return None

    user_prompt = ENRICHMENT_USER_TEMPLATE.format(
        product_name=_safe_str(product_name),
        description=_safe_str(description),
        specification=_safe_str(specification),
        category=_safe_str(category),
        packaging=_safe_str(packaging),
    )

    try:
        response = client.messages.create(
            model=AI_MODEL,
            max_tokens=3000,
            system=ENRICHMENT_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )

        text = next(
            (b.text for b in response.content if b.type == "text"), ""
        )
        result = _parse_json_response(text)
        if not result:
            logger.error(f"Failed to parse AI enrichment response: {text[:200]}")
            return None

        return result

    except anthropic.APIError as e:
        logger.error(f"Claude API error during enrichment: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during AI enrichment: {e}")
        return None


async def score_product_async(
    product_name: Optional[str] = None,
    description: Optional[str] = None,
    specification: Optional[str] = None,
    category: Optional[str] = None,
    packaging: Optional[str] = None,
) -> Optional[dict]:
    """Async version of score_product."""
    api_key = ANTHROPIC_API_KEY
    if not api_key:
        return None

    client = anthropic.AsyncAnthropic(api_key=api_key)

    user_prompt = SCORING_USER_TEMPLATE.format(
        product_name=_safe_str(product_name),
        description=_safe_str(description),
        specification=_safe_str(specification),
        category=_safe_str(category),
        packaging=_safe_str(packaging),
    )

    try:
        response = await client.messages.create(
            model=AI_MODEL,
            max_tokens=2000,
            system=SCORING_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )

        text = next(
            (b.text for b in response.content if b.type == "text"), ""
        )
        result = _parse_json_response(text)
        if not result:
            logger.error(f"Failed to parse AI scoring response: {text[:200]}")
            return None

        if "overall_score" not in result or "field_scores" not in result:
            logger.error(f"AI scoring response missing required fields")
            return None

        result["overall_score"] = max(0, min(100, result["overall_score"]))
        for key in result.get("field_scores", {}):
            result["field_scores"][key] = max(
                0, min(100, result["field_scores"][key])
            )

        return result

    except anthropic.APIError as e:
        logger.error(f"Claude API error during async scoring: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during async AI scoring: {e}")
        return None


async def enrich_product_async(
    product_name: Optional[str] = None,
    description: Optional[str] = None,
    specification: Optional[str] = None,
    category: Optional[str] = None,
    packaging: Optional[str] = None,
) -> Optional[dict]:
    """Async version of enrich_product."""
    api_key = ANTHROPIC_API_KEY
    if not api_key:
        return None

    client = anthropic.AsyncAnthropic(api_key=api_key)

    user_prompt = ENRICHMENT_USER_TEMPLATE.format(
        product_name=_safe_str(product_name),
        description=_safe_str(description),
        specification=_safe_str(specification),
        category=_safe_str(category),
        packaging=_safe_str(packaging),
    )

    try:
        response = await client.messages.create(
            model=AI_MODEL,
            max_tokens=3000,
            system=ENRICHMENT_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )

        text = next(
            (b.text for b in response.content if b.type == "text"), ""
        )
        result = _parse_json_response(text)
        if not result:
            logger.error(f"Failed to parse AI enrichment response: {text[:200]}")
            return None

        return result

    except anthropic.APIError as e:
        logger.error(f"Claude API error during async enrichment: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during async AI enrichment: {e}")
        return None


# ── AI REVIEW / QUALITY GATE ──
# This is the editorial quality layer. It receives source-grounded suggestions
# and polishes them into production-quality Norwegian masterdata.

REVIEW_SYSTEM_PROMPT = """Du er en redaktør for medisinsk produktmasterdata på norsk.

Du mottar forslag til forbedringer for produktdata-felt. Hvert forslag har:
- felt (field_name): hvilket felt forslaget gjelder
- nåværende verdi (current_value): hva som står i dag
- foreslått verdi (suggested_value): forslaget fra kilden
- kilde (source): hvor forslaget kommer fra
- kildeutdrag (evidence): utdrag fra kilden som støtter forslaget

Din oppgave er å KVALITETSSIKRE og FORBEDRE hvert forslag:

## REGLER PER FELT

### Produktnavn
- Kort, tydelig produkttittel
- Normaliser store/små bokstaver
- Fjern unødvendige tillegg
- ALDRI lag en setning — kun tittel

### Beskrivelse
- Skriv om til en WEBSHOP-KLAR produktbeskrivelse på norsk
- 2–4 korte, informative avsnitt
- Forklar bruksområde, egenskaper, materialer og fordeler
- Fjern ALL støy: tabelldata, artikkelnumre, pakkeinformasjon, metadata
- Fjern tall som ser ut som SKU-er eller bestillingsnumre
- Fjern "Side 1", datoer, URL-er, copyright
- Hvis kildeteksten er på engelsk, oversett til godt norsk
- Behold kun FAKTISK produktinformasjon fra kilden
- ALDRI legg til informasjon som ikke finnes i kilden
- Resultatet skal være klart for publisering i en nettbutikk

### Spesifikasjon
- Strukturert format: "Egenskap: Verdi; Egenskap: Verdi"
- Kun faktiske tekniske data
- Fjern markedsføringsspråk
- Behold alle verdier fra kilden

### Pakningsinformasjon
- KUN pakningsrelatert informasjon
- Format: "X stk per eske, Y esker per kartong"
- AVVIS hvis innholdet er lagringsinfo, bruksinstruksjoner eller markedsføring

### Produsent / Produsentens varenummer
- Kort, presis verdi
- Fjern overflødig tekst

### Kategori
- Hierarkisk format når mulig: "Hovedkategori > Underkategori"

## KRITISKE REGLER
- ALDRI finn opp fakta
- ALDRI legg til medisinsk informasjon som ikke finnes i kilden
- Hvis forslaget er for dårlig til å rettes, sett "rejected": true
- Forklar kort hvorfor i "reject_reason"
- Behold kildeinformasjon nøyaktig
- Skriv profesjonelt norsk

## RESPONS
Svar ALLTID med gyldig JSON — en liste med ett objekt per forslag."""

REVIEW_USER_TEMPLATE = """Kvalitetssikre følgende forslag for produktet "{product_name}" (art.nr: {article_number}):

{suggestions_json}

Svar med denne JSON-strukturen (INGEN annen tekst, kun JSON):
[
  {{
    "field_name": "<feltnavn>",
    "reviewed_value": "<forbedret verdi eller null hvis avvist>",
    "rejected": <true/false>,
    "reject_reason": "<grunn hvis avvist, ellers null>",
    "confidence_adjustment": <-0.2 til +0.1>,
    "review_required": <true/false>,
    "rationale": "<kort forklaring av endring>"
  }}
]"""


async def review_suggestions_async(
    article_number: str,
    product_name: Optional[str],
    suggestions: list[dict],
) -> Optional[list[dict]]:
    """AI editorial review of enrichment suggestions.

    Takes source-grounded suggestions and returns polished versions.
    Acts as a quality gate — rejects unusable suggestions, improves language,
    ensures field-correctness.

    Each suggestion dict should have:
      field_name, current_value, suggested_value, source, evidence

    Returns list of reviewed suggestions or None if AI unavailable.
    """
    api_key = ANTHROPIC_API_KEY
    if not api_key:
        return None

    if not suggestions:
        return []

    client = anthropic.AsyncAnthropic(api_key=api_key)

    # Build the suggestions JSON for the prompt
    suggestions_for_prompt = []
    for s in suggestions:
        suggestions_for_prompt.append({
            "field_name": s.get("field_name", ""),
            "current_value": s.get("current_value") or "(mangler)",
            "suggested_value": s.get("suggested_value", ""),
            "source": s.get("source", ""),
            "evidence": (s.get("evidence") or "")[:300],  # Limit evidence length
        })

    user_prompt = REVIEW_USER_TEMPLATE.format(
        product_name=_safe_str(product_name),
        article_number=article_number,
        suggestions_json=json.dumps(suggestions_for_prompt, ensure_ascii=False, indent=2),
    )

    try:
        response = await client.messages.create(
            model=AI_MODEL,
            max_tokens=3000,
            system=REVIEW_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )

        text = next(
            (b.text for b in response.content if b.type == "text"), ""
        )
        result = _parse_json_response(text)
        if not result:
            # Try parsing as a list
            text = text.strip()
            if text.startswith("["):
                try:
                    result = json.loads(text)
                except json.JSONDecodeError:
                    pass
        if not result:
            logger.error(f"Failed to parse AI review response: {text[:200]}")
            return None

        # Ensure it's a list
        if isinstance(result, dict):
            result = [result]
        if not isinstance(result, list):
            logger.error(f"AI review response is not a list: {type(result)}")
            return None

        logger.info(
            f"AI review for {article_number}: "
            f"{len(result)} suggestions reviewed, "
            f"{sum(1 for r in result if r.get('rejected'))} rejected"
        )
        return result

    except anthropic.APIError as e:
        logger.error(f"Claude API error during review: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during AI review: {e}")
        return None
