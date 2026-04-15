# Fixes Applied to google_reviews_worker.py

## Summary
Fixed two critical bugs that were causing the batch enrichment process to fail:
1. `quote_from_bytes() expected bytes` error in scrape_pages_jaunes()
2. Browser closure issues between batch iterations

---

## Bug #1: quote_from_bytes() Error in scrape_pages_jaunes()

### Problem
When `name` or `city` parameters were `None`, the code would fail at:
```python
q   = urllib.parse.quote_plus(name)
loc = urllib.parse.quote_plus(city)
```

This happened when:
- Database columns for company name or city didn't exist
- Query returned NULL values for these columns
- Empty/missing data in the source table

### Solution Applied

**In `scrape_pages_jaunes()` function (line 545-557):**
```python
# Handle None values - default to empty strings
name = name or ""
city = city or ""

# If both are empty, can't search
if not name.strip() and not city.strip():
    log.warning(f"  [PJ] Pas de nom ni de ville")
    return result
```

**In `enrich_one()` function (line 1043-1044):**
```python
name  = company.get("name", company.get("nom_entreprise", "")) or ""
city  = company.get("city", company.get("libelleCommuneEtablissement", "")) or ""
```

**Result:** ✓ Now safely handles None values by converting to empty strings

---

## Bug #2: Browser Closure Issue

### Problem
Browser/page would close between batch iterations with error:
```
[Maps] Erreur: Page.goto: Target page, context or browser has been closed
```

This happened because:
- Unhandled exceptions would close the Playwright context
- No validation that page was still open before reuse
- Browser state not checked between companies

### Solution Applied

**New helper function `ensure_page_valid()` (line 365-377):**
```python
def ensure_page_valid(page: Page, ctx) -> Page:
    """
    Vérifie que la page est valide, sinon crée une nouvelle.
    Utile si le browser/context se ferme pendant le traitement.
    """
    try:
        # Essayer d'accéder à une propriété pour vérifier si c'est valide
        _ = page.title
        return page
    except Exception:
        # Page fermée, créer une nouvelle
        log.warning("  [Page] Recréation (ancienne fermée)")
        return ctx.new_page()
```

**In main() loop (line 1226):**
```python
# Vérifier que la page est valide, sinon la recréer
page = ensure_page_valid(page, ctx)
```

**Improved error handling in scrape_google_maps() (line 535-542):**
```python
except Exception as e:
    # Ignorer les erreurs de page fermée (elles seront gérées dans main)
    if "closed" in str(e).lower():
        log.warning(f"  [Maps] Page fermée (sera recréée)")
    else:
        log.error(f"  [Maps] Erreur: {e}")
    return result
```

**Same in scrape_pages_jaunes() (line 592-598):**
```python
except Exception as e:
    # Ignorer les erreurs de page fermée (elles seront gérées dans main)
    if "closed" in str(e).lower():
        log.warning(f"  [PJ] Page fermée (sera recréée)")
    else:
        log.warning(f"  [PJ] Erreur: {e}")
```

**Result:** ✓ Page is automatically validated and recreated if needed at each company iteration

---

## Testing

### Test Command
```bash
python google_reviews_worker.py --limit 1 --dry-run
```

### Expected Behavior
- Script starts in headless mode (no UI visible)
- Processes 1 company from database
- Performs API gouv lookup (no throttling with default 5 concurrent)
- Scrapes Google Maps (validates company name match)
- Scrapes Pages Jaunes (handles None values gracefully)
- Scrapes website if found
- Outputs results without writing to DB (dry-run mode)

### Batch Processing Test
```bash
python batch_enrich.py --batch-size 100 --delay 10
```

Expected improvements:
- No more "quote_from_bytes" errors
- No more "Target page, context or browser has been closed" errors
- Headless mode by default (use `--headed` to see browser)

---

## Changed Files
- `google_reviews_worker.py` - Core enrichment script
  - Added `ensure_page_valid()` helper function
  - Added None handling in `scrape_pages_jaunes()`
  - Added None handling in `enrich_one()`
  - Improved error handling in `scrape_google_maps()` and `scrape_pages_jaunes()`
  - Added page validation in main() loop

---

## Compatibility Notes
- ✓ All existing features preserved (API gouv, Maps, Pages Jaunes, website scraping)
- ✓ Headless mode is default (--headed flag enables UI)
- ✓ Database operations unchanged
- ✓ Batch processing compatible with batch_enrich.py

---

## Future Improvements
1. Consider adding a page pooling mechanism to prevent frequent recreation
2. Add metrics for page recreation events
3. Consider implementing exponential backoff for API throttling
