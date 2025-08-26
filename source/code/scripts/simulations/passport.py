import random, string
from typing import List, Dict
from faker import Faker

EUROPEAN_COUNTRIES = [
    "Albania","Andorra","Armenia","Austria","Azerbaijan","Belarus","Belgium","Bosnia and Herzegovina",
    "Bulgaria","Croatia","Cyprus","Czechia","Estonia","Finland","France","Georgia","Germany","Greece",
    "Hungary","Iceland","Ireland","Italy","Kazakhstan","Kosovo","Latvia","Liechtenstein","Lithuania",
    "Luxembourg","Malta","Moldova","Monaco","Montenegro","Netherlands","North Macedonia","Norway",
    "Poland","Portugal","Romania","Russia","San Marino","Serbia","Slovakia","Slovenia","Spain",
    "Switzerland","Turkey","Ukraine","United Kingdom","Vatican City"
]

COUNTRY_TO_LOCALES = {
    "Denmark": ["da_DK"],
    "Sweden": ["sv_SE"],
    "Norway": ["nb_NO"],              
    "Finland": ["fi_FI"],
    "Germany": ["de_DE"],
    "Netherlands": ["nl_NL"],
    "France": ["fr_FR"],
    "Spain": ["es_ES"],
    "Portugal": ["pt_PT"],
    "Italy": ["it_IT"],
    "Poland": ["pl_PL"],
    "Czechia": ["cs_CZ"],
    "Slovakia": ["sk_SK"],
    "Slovenia": ["sl_SI"],
    "Hungary": ["hu_HU"],
    "Romania": ["ro_RO"],
    "Bulgaria": ["bg_BG"],
    "Greece": ["el_GR"],
    "Ireland": ["en_IE"],
    "United Kingdom": ["en_GB"],
    "Austria": ["de_AT"],
    "Switzerland": ["de_CH", "fr_CH", "it_CH"],
    "Russia": ["ru_RU"],
    "Ukraine": ["uk_UA"],
    "Turkey": ["tr_TR"],
    "Iceland": ["is_IS"],
    "_WORLD": ["en_GB", "en_US", "es_MX", "pt_BR", "ru_RU", "zh_CN", "ja_JP", "ko_KR", "hi_IN"],
}

WORLD_SAMPLE = [
    "United States","Canada","Mexico","Brazil","Argentina","South Africa","Egypt","Nigeria",
    "India","China","Japan","South Korea","Australia","New Zealand","Saudi Arabia","United Arab Emirates"
]

_FAKERS = {}

def _get_faker(locale: str) -> Faker:
    if locale not in _FAKERS:
        try:
            _FAKERS[locale] = Faker(locale)
        except Exception:
            _FAKERS[locale] = Faker("en_GB")
    return _FAKERS[locale]

def pick_locale_for_country(country: str) -> str:
    candidates = COUNTRY_TO_LOCALES.get(country) or COUNTRY_TO_LOCALES["_WORLD"]
    return random.choice(candidates)

def random_passport_number(length: int = 9) -> str:
    chars = string.ascii_uppercase + string.digits
    return "".join(random.choices(chars, k=length))

def pick_country() -> str:
    r = random.random()
    if r < 0.4:
        return "Denmark"
    elif r < 0.6:
        return "Sweden"
    elif r < 0.90:
        return random.choice([c for c in EUROPEAN_COUNTRIES if c not in ("Denmark", "Sweden")])
    else:
        return random.choice(WORLD_SAMPLE)

def make_name_for_country(country: str) -> str:
    locale = pick_locale_for_country(country)
    f = _get_faker(locale)
    return f.name()

def generate_passports_list(n: int, seed: int | None = None) -> List[Dict[str, str]]:
    """
    Return a list of dicts with unique passport numbers.
    """
    if seed is not None:
        random.seed(seed)

    results: List[Dict[str, str]] = []
    used_passports: set[str] = set()

    while len(results) < n:
        country = pick_country()
        name = make_name_for_country(country)

        # Ensure uniqueness
        pno = random_passport_number()
        while pno in used_passports:
            pno = random_passport_number()

        used_passports.add(pno)
        results.append({
            "passport_number": pno,
            "name": name,
            "country": country
        })
    return results

def main(n=1000000):
    return generate_passports_list(n = n)

if __name__ == "__main__":
    data = main(n = 10)
    print(data)