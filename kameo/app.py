import pandas as pd
import re
from pathlib import Path

# === LES FILEN OG PARSE INNHOLD ===
with open(Path("./input/investments.txt"), encoding="utf-8") as f:
    content = f.read()

# Forklaring på etterfølgende regex
# Splitt i blokker per lån
# Eksempel: "BFM 8 AS - 4481 | Løpetid: 26 m | Rente: 14,00%"
# Regexen leter etter linjer som begynner med tekst etterfulgt av " - tall | Løpetid"
blocks = re.split(r'\n(?=\w.+ - \d+ \| L\u00f8petid)', content)

rows = []
for block in blocks:
    lines = block.strip().split('\n')
    header = lines[0]
    # Forklaring på etterfølgende regex
    # Eksempel: "BFM 8 AS - 4481 | Løpetid: 26 m | Rente: 14,00%"
    # (.*?)            => selskapets navn
    # (\d+)            => loan_id
    # (\d+)            => løpetid i måneder
    # ([\d,]+)         => renteprosent (f.eks. "14,00")
    match = re.match(r"(.*?) - (\d+)\s+\| L\u00f8petid: (\d+)[^|]+\| Rente: ([\d,]+)%", header)
    if not match:
        continue
    company, loan_id, duration, interest = match.groups()

    for line in lines:
        # Forklaring på etterfølgende regex
        # Eksempel på transaksjonslinje: "2024-02-06\tTildeling\t−2000,00\tNOK\t100,00\t−2000,00"
        # Regex: linjer som starter med dato-format YYYY-MM-DD etterfulgt av tab (\t)
        if re.match(r"^\d{4}-\d{2}-\d{2}\t", line):
            parts = line.split('\t')
            if len(parts) == 6:
                date_str, txn_type, amount, _, _, amount_nok = parts
                try:
                    rows.append({
                        "company": company.strip(),
                        "loan_id": int(loan_id),
                        "duration_months": int(duration),
                        "interest_rate": float(interest.replace(",", ".")),
                        "date": pd.to_datetime(date_str, format="%Y-%m-%d"),
                        "transaction_type": txn_type.strip(),
                        "amount_nok": float(amount_nok.replace(",", ".").replace("\u2212", "-").replace(" ", ""))
                    })
                except:
                    continue

# Lag rå dataframe
raw_df = pd.DataFrame(rows)
pd.set_option("display.max_columns", None)

# === BY LOAN DATAFRAME ===
by_loan_df = raw_df.copy()

def estimate_repaid(group, **kwargs):
    tilbakebetaling_date = group[group["transaction_type"] == "Tilbakebetaling"]["date"].min()
    if pd.notnull(tilbakebetaling_date):
        return tilbakebetaling_date
    rente_date = group[group["transaction_type"] == "Renteinntekt"]["date"].min()
    duration = group["duration_months"].iloc[0]
    if pd.notnull(rente_date):
        # Første Renteinntekt + antall terminer
        return rente_date + pd.DateOffset(months=duration)
    else:
        # Første dato pr lån + antall terminer + 1 måned
        first_date = group["date"].min()
        return first_date + pd.DateOffset(months=duration + 1)

estimated_dates = by_loan_df.drop(columns=["loan_id"]).groupby(by_loan_df["loan_id"], group_keys=False).apply(estimate_repaid).reset_index(name="estimated_repaid")
by_loan_df = by_loan_df.merge(estimated_dates, on="loan_id", how="left")

# Formatér datoer som YYYY-MM-DD
by_loan_df["date"] = by_loan_df["date"].dt.strftime("%Y-%m-%d")
by_loan_df["estimated_repaid"] = by_loan_df["estimated_repaid"].dt.strftime("%Y-%m-%d")

# Legg til status-kolonne basert på transaksjonstyper
# 'Tilbakebetalt' hvis "Tilbakebetaling" finnes
# 'Venter' hvis "Tildeling" finnes men ikke "Renteinntekt"
# Ellers 'Aktiv'
def loan_status(group, **kwargs):
    txn_types = group["transaction_type"].values
    if "Tilbakebetaling" in txn_types:
        return "Tilbakebetalt"
    elif "Tildeling" in txn_types and "Renteinntekt" not in txn_types:
        return "Venter"
    else:
        return "Aktiv"

status_df = by_loan_df.drop(columns=["loan_id"]).groupby(by_loan_df["loan_id"]).apply(loan_status).reset_index(name="status")
by_loan_df = by_loan_df.merge(status_df, on="loan_id", how="left")

# Legg til kolonne som teller antall Renteinntekter pr lån, teller oppover for hver dato

# Beregn total forventet renteinntekt per rad basert på interest_rate, Tildeling og duration_months
def calc_forventet_rente(row):
    # Finn tildelt beløp for dette lånet, alltid positiv
    tildeling = abs(by_loan_df[(by_loan_df["loan_id"] == row["loan_id"]) & (by_loan_df["transaction_type"] == "Tildeling")]["amount_nok"].sum())
    rente = row["interest_rate"] / 100
    duration_years = row["duration_months"] / 12
    return tildeling * rente * duration_years

by_loan_df["forventet_renteinntekt"] = by_loan_df.apply(calc_forventet_rente, axis=1)

by_loan_df = by_loan_df.sort_values(["loan_id", "date"])
by_loan_df["innbetalte_terminer"] = by_loan_df.groupby("loan_id")["transaction_type"].transform(lambda x: (x == "Renteinntekt").cumsum())


# Beregn netto renteinntekt per lån
forsinkelsesrente = by_loan_df.groupby("loan_id")["amount_nok"].apply(lambda x: x[by_loan_df.loc[x.index, "transaction_type"] == "Forsinkelsesrente"].sum())
renteinntekt = by_loan_df.groupby("loan_id")["amount_nok"].apply(lambda x: x[by_loan_df.loc[x.index, "transaction_type"] == "Renteinntekt"].sum())
by_loan_df["netto_renteinntekt"] = by_loan_df["loan_id"].map(forsinkelsesrente + renteinntekt)




# Utestående renter (forventet - netto)
by_loan_df["renter_utestaaende"] = by_loan_df["forventet_renteinntekt"] - by_loan_df["netto_renteinntekt"]

# Sammenligning netto renteinntekt vs total forventet renteinntekt (prosent)
by_loan_df["netto_vs_forventet_renteinntekt_prosent"] = (by_loan_df["netto_renteinntekt"] / by_loan_df["forventet_renteinntekt"])*100

# Tildeling per lån (alltid positiv)
by_loan_df["tildeling"] = abs(by_loan_df.groupby("loan_id")["amount_nok"].transform(lambda x: x[by_loan_df.loc[x.index, "transaction_type"] == "Tildeling"].sum()))

# Total forventet avkastning (tildeling + forventet renteinntekt)
by_loan_df["total_forventet_avkastning"] = by_loan_df["tildeling"] + by_loan_df["forventet_renteinntekt"]

# Tilbakebetalt beløp per lån (alltid positiv)
by_loan_df["tilbakebetalt"] = abs(by_loan_df.groupby("loan_id")["amount_nok"].transform(lambda x: x[by_loan_df.loc[x.index, "transaction_type"] == "Tilbakebetaling"].sum()))

# Total faktisk avkastning (tilbakebetalt + netto renteinntekt)
by_loan_df["total_faktisk_avkastning"] = by_loan_df["tilbakebetalt"] + by_loan_df["netto_renteinntekt"]

# Ratio mellom total faktisk avkastning og total forventet avkastning (prosent)
by_loan_df["faktisk_vs_forventet_avkastning_prosent"] = (by_loan_df["total_faktisk_avkastning"] / by_loan_df["total_forventet_avkastning"])*100

# Sorter kolonnene i ønsket rekkefølge



loan_columns = [
    "company", "loan_id", "status", "duration_months", "innbetalte_terminer",
    "estimated_repaid", "interest_rate", "forventet_renteinntekt", "netto_renteinntekt", "renter_utestaaende", "netto_vs_forventet_renteinntekt_prosent",
    "tildeling", "total_forventet_avkastning", "tilbakebetalt", "total_faktisk_avkastning", "faktisk_vs_forventet_avkastning_prosent",
    "date", "transaction_type", "amount_nok"
]
by_loan_df = by_loan_df[loan_columns]

# Eksporter og print til Excel og CSV for testing
by_loan_df.to_excel("transformed_kameo.xlsx", index=False)
by_loan_df.to_csv("transformed_kameo.csv", index=False, encoding="utf-8")


# Aggregert by_lender_df med riktige summer og kolonner
amounts_pivot = by_loan_df.pivot_table(
    index=["company", "status"],
    columns="transaction_type",
    values="amount_nok",
    aggfunc="sum",
    fill_value=0
).reset_index()


# Først: max pr lån for alle relevante kolonner
max_per_loan = by_loan_df.groupby(["loan_id"]).agg({
    "forventet_renteinntekt": "max",
    "netto_renteinntekt": "max",
    "tildeling": "max",
    "tilbakebetalt": "max",
    "company": "first",
    "status": "first",
    "interest_rate": "first",
    "date": "max",
    "total_forventet_avkastning": "max",
    "total_faktisk_avkastning": "max"
}).reset_index()

# Deretter: sum pr company og status
by_lender_df = max_per_loan.groupby(["company", "status"]).agg({
    "loan_id": "nunique",
    "interest_rate": "mean",
    "forventet_renteinntekt": "sum",
    "netto_renteinntekt": "sum",
    "tildeling": "sum",
    "tilbakebetalt": "sum",
    "date": "max",
    "total_forventet_avkastning": "sum",
    "total_faktisk_avkastning": "sum"
}).reset_index()

# Regn ut prosentene på nytt
by_lender_df["renter_utestaaende"] = by_lender_df["forventet_renteinntekt"] - by_lender_df["netto_renteinntekt"]
by_lender_df["netto_vs_forventet_renteinntekt_prosent"] = (by_lender_df["netto_renteinntekt"] / by_lender_df["forventet_renteinntekt"]).replace([float('inf'), -float('inf')], 0) * 100
by_lender_df["faktisk_vs_forventet_avkastning_prosent"] = (by_lender_df["total_faktisk_avkastning"] / by_lender_df["total_forventet_avkastning"]).replace([float('inf'), -float('inf')], 0) * 100

by_lender_df = by_lender_df.merge(amounts_pivot, on=["company", "status"], how="left")

# Netto renteinntekt (sum av Forsinkelsesrente og Renteinntekt)
by_lender_df["Netto_renteinntekt"] = by_lender_df.get("Forsinkelsesrente", 0) + by_lender_df.get("Renteinntekt", 0)

# Gi kolonnene riktige navn og rekkefølge
by_lender_df = by_lender_df.rename(columns={
    "company": "company",
    "status": "status",
    "date": "siste_transaksjonsdato",
    "loan_id": "antall_laan",
    "interest_rate": "gjennomsnittlig_rente",
    "forventet_renteinntekt": "forventet_renteinntekt",
    "Tildeling": "Tildeling",
    "Tilbakebetaling": "Tilbakebetalt",
    "Forsinkelsesrente": "forsinkelses_rente",
    "Renteinntekt": "Renteinntekt"
})

by_lender_df = by_lender_df[[
    "company", "status", "siste_transaksjonsdato", "antall_laan", "gjennomsnittlig_rente",
    "forventet_renteinntekt", "Tildeling", "Tilbakebetalt", "forsinkelses_rente", "Renteinntekt", "Netto_renteinntekt"
]]



# Eksporter og print til Excel og CSV for testing
by_lender_df.to_excel("transformed_kameo_lender.xlsx", index=False)
by_lender_df.to_csv("transformed_kameo_lender.csv", index=False, encoding="utf-8")
print(by_lender_df.head(5))
