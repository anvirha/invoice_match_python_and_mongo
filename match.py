from pymongo import MongoClient
import re
from datetime import datetime
from difflib import SequenceMatcher

# === MongoDB Configuration ===
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "my_matcher"
PARSED_COLLECTION = "parsed_data"
TWO_B_COLLECTION = "two_b"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
parsed_data_collection = db[PARSED_COLLECTION]
two_b_data_collection = db[TWO_B_COLLECTION]

# === Cleaning Function ===
def clean_string(value):
    return re.sub(r'\W+', '', str(value)).lower()

# === Fetch Batches ===
def get_batches():
    try:
        cursor = parsed_data_collection.find(
            {
                "processed_data.invoice_number": {"$exists": True}
            },
            {
                "processed_data": 1
            }
        ).limit(5)

        b1 = []
        b1_invoices = []  # to store raw invoice numbers for printing

        for doc in cursor:
            processed = doc.get("processed_data", {})
            if processed.get("invoice_number"):
                inv = processed.get("invoice_number", "")
                b1.append({
                    "raw_invoice": inv,
                    "clean_invoice": clean_string(inv),
                    "clean_hotel_gstin": clean_string(processed.get("hotel_gstin", "")),
                    "clean_guest_gstin": clean_string(processed.get("guest_gstin", "")),
                    "checkout_date": processed.get("checkout_date"),
                    "invoice_amount": processed.get("invoice_amount"),
                    "source": "processed_data"
                })
                b1_invoices.append(inv)

        cursor2 = two_b_data_collection.find(
            {
                "two_b_matches.inum": {"$exists": True}
            },
            {
                "two_b_matches": 1
            }
        ).limit(10)

        b2 = []
        b2_invoices = []  # to store raw inum values for printing

        for doc in cursor2:
            matches_list = doc.get("two_b_matches", [])
            # Handle if two_b_matches is a list of dicts
            if isinstance(matches_list, list):
                for two_b_match in matches_list:
                    if two_b_match.get("inum"):
                        b2.append({
                            "raw_inum": two_b_match.get("inum", ""),
                            "clean_inum": clean_string(two_b_match.get("inum", "")),
                            "clean_ctin": clean_string(two_b_match.get("ctin", "")),
                            "clean_gstin": clean_string(two_b_match.get("gstin", "")),
                            "dt": two_b_match.get("dt"),
                            "val": two_b_match.get("val"),
                            "source": "two_b_matches"
                        })
                        b2_invoices.append(two_b_match.get("inum", ""))
            # In case two_b_matches is a single dict (rare)
            elif isinstance(matches_list, dict):
                two_b_match = matches_list
                if two_b_match.get("inum"):
                    b2.append({
                        "raw_inum": two_b_match.get("inum", ""),
                        "clean_inum": clean_string(two_b_match.get("inum", "")),
                        "clean_ctin": clean_string(two_b_match.get("ctin", "")),
                        "clean_gstin": clean_string(two_b_match.get("gstin", "")),
                        "dt": two_b_match.get("dt"),
                        "val": two_b_match.get("val"),
                        "source": "two_b_matches"
                    })
                    b2_invoices.append(two_b_match.get("inum", ""))

        print(f"Batch 1 Invoices (processed_data): {b1_invoices}")
        

        return b1, b2

    except Exception as e:
        print(f"⚠️ Error reading from MongoDB: {e}")
        return [], []

# === Fuzzy Match Function ===
def fuzzy_match(a, b, threshold=0.8):
    return SequenceMatcher(None, a, b).ratio() >= threshold

# === Updated Match Function with multiple matches handling ===
def match_batches(b1, b2):
    matches = []

    def get_date_diff(d1_raw, d2_raw):
        try:
            d1 = datetime.fromisoformat(d1_raw.replace("Z", "+00:00")) if d1_raw else None

            d2 = d2_raw
            if isinstance(d2, dict) and "$date" in d2:
                d2 = datetime.fromisoformat(d2["$date"].replace("Z", "+00:00"))
            elif isinstance(d2, str):
                d2 = datetime.fromisoformat(d2.replace("Z", "+00:00"))
            else:
                d2 = None

            if d1 and d2:
                return abs((d1.date() - d2.date()).days)
        except:
            return None
        return None

    def get_amount_diff(a1, a2):
        try:
            amount1 = float(a1)
            amount2 = float(a2)
            return round(abs(amount1 - amount2), 2)
        except:
            return None

    for item1 in b1:
        # === Step 1: Try exact match ===
        exact_matches = [item2 for item2 in b2 if item1["clean_invoice"] == item2["clean_inum"]]
        match_method = "exact"

        # === Step 2: Try fuzzy match ===
        if not exact_matches:
            exact_matches = [item2 for item2 in b2 if fuzzy_match(item1["clean_invoice"], item2["clean_inum"])]
            match_method = "fuzzy"

        # === Step 3: Handle single match ===
        if len(exact_matches) == 1:
            matched_item = exact_matches[0]
            match_type = "exact"

            hotel_match = item1["clean_hotel_gstin"] == matched_item["clean_ctin"]
            guest_match = item1["clean_guest_gstin"] == matched_item["clean_gstin"]
            date_diff = get_date_diff(item1.get("checkout_date", ""), matched_item["dt"])
            amount_diff = get_amount_diff(item1.get("invoice_amount"), matched_item["val"])

            matches.append({
                "raw_invoice": item1["raw_invoice"],
                "invoice_match": True,
                "match_count": 1,
                "match_method": match_method,
                "hotel_gstin_match": hotel_match,
                "guest_gstin_match": guest_match,
                "matched_inum": matched_item["raw_inum"],
                "date_diff": date_diff,
                "amount_diff": amount_diff,
                "match_type": match_type
            })

        # === Step 4: Handle multiple matches ===
        elif len(exact_matches) > 1:
            hotel_gstin_matches = [m for m in exact_matches if item1["clean_hotel_gstin"] == m["clean_ctin"]]
            guest_gstin_matches = [m for m in exact_matches if item1["clean_guest_gstin"] == m["clean_gstin"]]

            candidate_matches = hotel_gstin_matches or guest_gstin_matches or exact_matches

            def score_match(m):
                date_diff = get_date_diff(item1.get("checkout_date", ""), m["dt"])
                amount_diff = get_amount_diff(item1.get("invoice_amount"), m["val"])
                date_diff = date_diff if date_diff is not None else 10000
                amount_diff = amount_diff if amount_diff is not None else 10000
                return date_diff * 1000 + amount_diff

            best_match = min(candidate_matches, key=score_match)
            matched_item = best_match

            hotel_match = item1["clean_hotel_gstin"] == matched_item["clean_ctin"]
            guest_match = item1["clean_guest_gstin"] == matched_item["clean_gstin"]
            date_diff = get_date_diff(item1.get("checkout_date", ""), matched_item["dt"])
            amount_diff = get_amount_diff(item1.get("invoice_amount"), matched_item["val"])

            matches.append({
                "raw_invoice": item1["raw_invoice"],
                "invoice_match": True,
                "match_count": len(exact_matches),
                "match_method": "multiple_best_match",
                "hotel_gstin_match": hotel_match,
                "guest_gstin_match": guest_match,
                "matched_inum": matched_item["raw_inum"],
                "date_diff": date_diff,
                "amount_diff": amount_diff,
                "match_type": "multiple_best_match"
            })

        # === Step 5: No invoice match → try GSTIN only
        else:
            gstin_matched = False
            for item2 in b2:
                hotel_match = item1["clean_hotel_gstin"] == item2["clean_ctin"]
                guest_match = item1["clean_guest_gstin"] == item2["clean_gstin"]

                if hotel_match or guest_match:
                    gstin_matched = True

                    date_diff = get_date_diff(item1.get("checkout_date", ""), item2["dt"])
                    amount_diff = get_amount_diff(item1.get("invoice_amount"), item2["val"])

                    matches.append({
                        "raw_invoice": item1["raw_invoice"],
                        "invoice_match": False,
                        "match_count": 0,
                        "match_method": "gstin_partial",
                        "hotel_gstin_match": hotel_match,
                        "guest_gstin_match": guest_match,
                        "matched_inum": item2["raw_inum"],
                        "date_diff": date_diff,
                        "amount_diff": amount_diff,
                        "match_type": "gstin_partial"
                    })
                    break

            if not gstin_matched:
                matches.append({
                    "raw_invoice": item1["raw_invoice"],
                    "invoice_match": False,
                    "match_count": 0,
                    "match_method": "unmatched",
                    "hotel_gstin_match": False,
                    "guest_gstin_match": False,
                    "matched_inum": None,
                    "date_diff": None,
                    "amount_diff": None,
                    "match_type": "unmatched"
                })

    # === Optional: Summary print
    for m in matches:
        print(f"Invoice: {m['raw_invoice']} | Matches: {m['match_count']} | Method: {m['match_method']}")

    return matches

# === Mongo Updater Function ===
def update_mongo(matches):
    for match in matches:
        raw_val = match["raw_invoice"]

        parsed_data_collection.update_many(
            {"processed_data.invoice_number": {"$regex": f"^{re.escape(raw_val)}$", "$options": "i"}},
            {
                "$set": {
                    "invoice_matched": match["invoice_match"],
                    "hotelgstin_matched": match["hotel_gstin_match"],
                    "guestgstin_matched": match["guest_gstin_match"],
                    "date_diff": match["date_diff"] if match["date_diff"] is not None else None,
                    "in_amount_diff": match["amount_diff"]
                }
            }
        )

        if match["invoice_match"] and match["matched_inum"]:
            two_b_data_collection.update_many(
                {"two_b_matches.inum": {"$regex": f"^{re.escape(match['matched_inum'])}$", "$options": "i"}},
                {
                    "$set": {
                        "invoice_received": True,
                        "ctin_received": match["hotel_gstin_match"],
                        "gstin_received": match["guest_gstin_match"],
                        "dt_diff": match["date_diff"] if match["date_diff"] is not None else None,
                        "val_diff": match["amount_diff"] if match["amount_diff"] is not None else None
                    }
                }
            )

        print(
            f"{raw_val}: "
            f"Invoice={'✅' if match['invoice_match'] else '❌'}, "
            f"Hotel GSTIN={'✅' if match['hotel_gstin_match'] else '❌'}, "
            f"Guest GSTIN={'✅' if match['guest_gstin_match'] else '❌'}, "
            f"Date Diff={match['date_diff']}, "
            f"Amount Diff={match['amount_diff']}"
        )

# === Execution ===
if __name__ == "__main__":
    batch_b1, batch_b2 = get_batches()
    match_results = match_batches(batch_b1, batch_b2)
    update_mongo(match_results)
    print("✅ Mongo update completed.")
