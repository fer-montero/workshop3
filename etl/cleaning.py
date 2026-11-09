"""Limpieza y armonización de los CSV de World Happiness (2015-2019)."""
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
YEARS = [2015, 2016, 2017, 2018, 2019]

# Cada concepto tiene un único nombre en el esquema unificado (coincide con el contrato del evento)
COLUMN_MAP = {
    "country":         ["Country", "Country or region"],
    "happiness_score": ["Happiness Score", "Happiness.Score", "Score"],
    "gdp":             ["Economy (GDP per Capita)", "Economy..GDP.per.Capita.", "GDP per capita"],
    "family":          ["Family", "Social support"],
    "health":          ["Health (Life Expectancy)", "Health..Life.Expectancy.", "Healthy life expectancy"],
    "freedom":         ["Freedom", "Freedom to make life choices"],
    "generosity":      ["Generosity"],
    "corruption":      ["Trust (Government Corruption)", "Trust..Government.Corruption.", "Perceptions of corruption"],
}
RENAME = {old: new for new, olds in COLUMN_MAP.items() for old in olds}

FEATURES = ["gdp", "family", "health", "freedom", "generosity", "corruption"]
UNIFIED_COLS = ["country", "year"] + FEATURES + ["happiness_score"]


def load_raw(year: int) -> pd.DataFrame:
    return pd.read_csv(RAW_DIR / f"{year}.csv", encoding="utf-8-sig")


def harmonize(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Renombra al esquema unificado, agrega el año y descarta columnas fuera del esquema."""
    out = df.rename(columns=RENAME)
    out["year"] = year
    out = out[UNIFIED_COLS].copy()
    out["country"] = out["country"].str.strip()
    return out


def build_unified() -> pd.DataFrame:
    return pd.concat([harmonize(load_raw(y), y) for y in YEARS], ignore_index=True)

COUNTRY_ALIASES = {
    "Hong Kong S.A.R., China": "Hong Kong",
    "Taiwan Province of China": "Taiwan",
    "Trinidad & Tobago": "Trinidad and Tobago",
    "Northern Cyprus": "North Cyprus",
    "Macedonia": "North Macedonia",
    "Somaliland region": "Somaliland Region",
}


def standardize_countries(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["country"] = out["country"].replace(COUNTRY_ALIASES)
    return out


def handle_missing(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina filas con nulos en las variables del modelo (1 fila: Emiratos Árabes 2018)."""
    return df.dropna(subset=FEATURES + ["happiness_score"]).reset_index(drop=True)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    out = standardize_countries(df)
    out = handle_missing(out)
    out = out.astype({"year": "int64", **{c: "float64" for c in FEATURES + ["happiness_score"]}})
    assert not out.duplicated(["country", "year"]).any(), "Hay países repetidos en un mismo año"
    return out

def inspect(df: pd.DataFrame) -> None:
    """Imprime los hallazgos que faltan por resolver (no modifica nada)."""
    print("\n=== Filas con nulos ===")
    print(df[df.isna().any(axis=1)].to_string())

    print("\n=== Países que no aparecen en los 5 años ===")
    years_count = df.groupby("country")["year"].nunique()
    partial = years_count[years_count < len(YEARS)].sort_values()
    print(f"En los 5 años: {(years_count == len(YEARS)).sum()} | en menos años: {len(partial)}")

    years_present = df.groupby("country")["year"].apply(
        lambda s: ", ".join(map(str, sorted(s)))
    )
    print(years_present[partial.index].to_string())

def save(df: pd.DataFrame, name: str = "happiness_unified.csv") -> Path:
    """Guarda el dataset limpio, ordenado para que el resultado sea reproducible."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    path = PROCESSED_DIR / name
    df.sort_values(["year", "country"]).to_csv(path, index=False, encoding="utf-8")
    return path

def profile_ranges_outliers(df: pd.DataFrame) -> None:
    """Imprime rangos y outliers (regla IQR) de las variables del modelo. No modifica nada."""
    cols = FEATURES + ["happiness_score"]
    rows = []
    masks = {}
    for c in cols:
        q1, q3 = df[c].quantile([0.25, 0.75])
        iqr = q3 - q1
        low, high = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        masks[c] = (df[c] < low) | (df[c] > high)
        rows.append({
            "columna": c,
            "min": df[c].min(),
            "max": df[c].max(),
            "negativos": int((df[c] < 0).sum()),
            "limite_inf_iqr": round(low, 3),
            "limite_sup_iqr": round(high, 3),
            "outliers_iqr": int(masks[c].sum()),
        })
    print("\n=== Rangos y outliers (regla IQR) ===")
    print(pd.DataFrame(rows).set_index("columna").to_string())

    fuera = int(((df["happiness_score"] < 0) | (df["happiness_score"] > 10)).sum())
    print(f"\nhappiness_score fuera de la escala 0-10: {fuera}")

    print("\n=== Los 5 outliers más extremos por columna ===")
    for c in cols:
        if masks[c].any():
            desviacion = (df.loc[masks[c], c] - df[c].median()).abs()
            top = desviacion.sort_values(ascending=False).head(5).index
            print(f"\n{c}:")
            print(df.loc[top, ["country", "year", c]].to_string(index=False))

if __name__ == "__main__":
    unified = build_unified()
    cleaned = clean(unified)
    print("Antes:", unified.shape, "| Después:", cleaned.shape)
    inspect(cleaned)
    profile_ranges_outliers(cleaned)
    path = save(cleaned)
    print("\nGuardado en:", path.relative_to(PROJECT_ROOT))