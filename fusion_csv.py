"""
fusion_csv.py
=============
Fusionne le CSV Notion (avec colonnes Plateforme + Liens)
avec resultats.json (données JustWatch).

UTILISATION
-----------
    python3 fusion_csv.py
    python3 fusion_csv.py --notion 1001_films_csv_xxx.csv --input resultats.json --output resultats.json
"""

import csv, json, re, unicodedata, argparse, sys
from difflib import SequenceMatcher
from datetime import datetime

# ─────────────────────────────────────────────
# NORMALISATION
# ─────────────────────────────────────────────

def normalize(s):
    s = s.lower().strip()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for art in ["le ", "la ", "les ", "l ", "un ", "une ", "the ", "a "]:
        if s.startswith(art):
            s = s[len(art):]
    return s

def similarity(a, b):
    return SequenceMatcher(None, normalize(a), normalize(b)).ratio()

# ─────────────────────────────────────────────
# PARSING PLATEFORME
# ─────────────────────────────────────────────

# Mapping vers noms canoniques + type
PLATFORM_MAP = {
    "youtube":       ("YouTube",        "Gratuit"),
    "dailymotion":   ("Dailymotion",    "Gratuit"),
    "ok":            ("OK.ru",          "Gratuit"),
    "arte":          ("Arte",           "Gratuit"),
    "france.tv":     ("France.tv",      "Gratuit"),
    "francetv":      ("France.tv",      "Gratuit"),
    "henri":         ("Henri",          "Gratuit"),
    "tf1+":          ("TF1+",           "Gratuit"),
    "tf1":           ("TF1+",           "Gratuit"),
    "netflix":       ("Netflix",        "Abonnement inclus"),
    "prime video":   ("Prime Video",    "Abonnement inclus"),
    "prime":         ("Prime Video",    "Abonnement inclus"),
    "disney+":       ("Disney+",        "Abonnement inclus"),
    "disney":        ("Disney+",        "Abonnement inclus"),
    "appletv":       ("Apple TV+",      "Abonnement inclus"),
    "apple tv":      ("Apple TV+",      "Abonnement inclus"),
    "canal+":        ("Canal+",         "Abonnement inclus"),
    "mubi":          ("Mubi",           "Abonnement inclus"),
    "max":           ("Max",            "Abonnement inclus"),
}

def parse_platform(plateforme_str):
    """Convertit 'Prime Video, Disney+' → [('Prime Video','Abonnement inclus'), ...]"""
    results = []
    parts = re.split(r"[,;/]", plateforme_str)
    for part in parts:
        part_clean = part.strip().lower()
        # Retire les suffixes courants
        part_clean = re.sub(r"\s*\(gratuit\)|\s*\(payant\)", "", part_clean).strip()
        for key, val in PLATFORM_MAP.items():
            if key in part_clean:
                if val not in results:
                    results.append(val)
                break
    return results

def extract_urls(liens_str):
    """Extrait les URLs d'une cellule Liens (peut contenir plusieurs URLs séparées par virgule/espace)."""
    if not liens_str:
        return []
    # Split sur virgule ou espace suivi de http
    urls = re.split(r",\s*|(?=https?://)", liens_str)
    return [u.strip() for u in urls if u.strip().startswith("http")]

# ─────────────────────────────────────────────
# LOAD CSV NOTION
# ─────────────────────────────────────────────

def load_notion_csv(path):
    films = []
    with open(path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            titre = (row.get("Titre") or "").strip()
            annee = (row.get("Année") or "").strip()
            plateforme = (row.get("Plateforme") or "").strip()
            liens = (row.get("Liens") or "").strip()
            affiche = (row.get("affiche_url") or "").strip()

            if not titre:
                continue

            platforms_parsed = parse_platform(plateforme) if plateforme else []
            urls = extract_urls(liens)

            enfant = (row.get("Enfants friendly") or "").strip()
        culte   = (row.get("Pourquoi ce film est culte ?") or "").strip()

        films.append({
                "titre":        titre,
                "annee":        annee,
                "platforms":    platforms_parsed,
                "urls":         urls,
                "affiche_url":  affiche,
                "plateforme_raw": plateforme,
                "enfant_friendly": enfant,
                "culte":        culte,
            })
    return films

# ─────────────────────────────────────────────
# MATCHING
# ─────────────────────────────────────────────

def find_match(notion_titre, notion_annee, jw_films, threshold=0.85):
    best_score, best_film = 0, None
    n = normalize(notion_titre)
    try:
        annee_int = int(float(notion_annee)) if notion_annee else None
    except:
        annee_int = None

    for film in jw_films:
        t  = normalize(film.get("titre", ""))
        jt = normalize(film.get("jw_title") or "")
        score = max(
            SequenceMatcher(None, n, t).ratio(),
            SequenceMatcher(None, n, jt).ratio(),
        )
        # Bonus si l'année correspond
        film_year = film.get("annee") or film.get("jw_year")
        try:
            fy = int(float(film_year)) if film_year else None
        except:
            fy = None
        if annee_int and fy and abs(annee_int - fy) <= 1:
            score = min(score + 0.05, 1.0)

        if score > best_score:
            best_score, best_film = score, film

    return (best_film, best_score) if best_score >= threshold else (None, best_score)

# ─────────────────────────────────────────────
# FUSION
# ─────────────────────────────────────────────

def merge(jw_data, notion_films):
    jw_films = jw_data["films"]
    matched = unmatched = enriched = 0

    for nf in notion_films:
        has_platform  = len(nf["platforms"]) > 0
        has_affiche   = bool(nf["affiche_url"])
        has_extra     = bool(nf.get("enfant_friendly") or nf.get("culte"))

        if not has_platform and not has_affiche and not has_extra:
            continue

        film, score = find_match(nf["titre"], nf["annee"], jw_films)

        if not film:
            unmatched += 1
            continue

        matched += 1

        # Enrichit les plateformes
        if has_platform:
            film.setdefault("platforms", {})
            for name, type_ in nf["platforms"]:
                if name not in film["platforms"]:
                    film["platforms"][name] = [type_]
                    enriched += 1
            # Associe les URLs aux plateformes
            for url in nf["urls"]:
                if "youtube" in url:
                    film["url_youtube"] = url
                elif "dailymotion" in url:
                    film["url_dailymotion"] = url
                elif "ok.ru" in url:
                    film["url_ok"] = url
                elif "arte.tv" in url:
                    film["url_arte"] = url
                elif "france.tv" in url:
                    film["url_francetv"] = url
                elif "tf1" in url:
                    film["url_tf1"] = url
                elif "cinematheque" in url or "henri" in url:
                    film["url_henri"] = url
                else:
                    film["url_other"] = url

            if film["platforms"]:
                film["available"] = True

        # Enrichit l'affiche si manquante
        if has_affiche and not film.get("affiche_url"):
            film["affiche_url"] = nf["affiche_url"]

        # Enrichit enfant_friendly et culte
        if nf.get("enfant_friendly") and not film.get("enfant_friendly"):
            film["enfant_friendly"] = nf["enfant_friendly"]
        if nf.get("culte") and not film.get("culte"):
            film["culte"] = nf["culte"]

    # Recalcule stats
    available = sum(1 for f in jw_films if f.get("available"))
    jw_data["films_disponibles_streaming"] = available
    jw_data["fusion_notion_csv"] = datetime.now().isoformat()
    jw_data["notion_matches"]   = matched
    jw_data["notion_unmatched"] = unmatched
    jw_data["plateformes_ajoutees"] = enriched

    return jw_data, matched, unmatched, enriched

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--notion", default="1001_films_csv_31372815309c8081851bc77cbdf60948_all.csv")
    p.add_argument("--input",  default="resultats.json")
    p.add_argument("--output", default="resultats.json")
    args = p.parse_args()

    print(f"\n{'═'*55}")
    print(f"  🎬  16/9 × Fusion CSV Notion + JustWatch")
    print(f"{'═'*55}\n")

    # Charge JustWatch
    print(f"  📂 Chargement de {args.input}...")
    with open(args.input, encoding="utf-8") as f:
        jw_data = json.load(f)
    print(f"  ✅ {jw_data['total_films']} films JustWatch\n")

    # Charge CSV Notion
    print(f"  📂 Chargement de {args.notion}...")
    notion_films = load_notion_csv(args.notion)
    with_platform = sum(1 for f in notion_films if f["platforms"])
    print(f"  ✅ {len(notion_films)} films Notion")
    print(f"  ✅ {with_platform} films avec plateforme renseignée\n")

    # Aperçu plateformes trouvées
    from collections import Counter
    counts = Counter()
    for nf in notion_films:
        for name, _ in nf["platforms"]:
            counts[name] += 1
    print("  Plateformes détectées dans le CSV :")
    for name, c in counts.most_common():
        print(f"    {c:3d}x  {name}")
    print()

    # Fusion
    print("  🔀 Fusion en cours...")
    jw_data, matched, unmatched, enriched = merge(jw_data, notion_films)

    # Sauvegarde
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(jw_data, f, ensure_ascii=False, indent=2)

    print(f"\n{'═'*55}")
    print(f"  Films matchés          : {matched}")
    print(f"  Sans correspondance    : {unmatched}")
    print(f"  Entrées plateforme +   : {enriched}")
    print(f"  Total dispo streaming  : {jw_data['films_disponibles_streaming']}")
    print(f"\n  ✅ Sauvegardé → {args.output}")
    print(f"{'═'*55}\n")

if __name__ == "__main__":
    main()
