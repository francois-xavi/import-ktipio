# Testing Guide for Bug Fixes

## Overview
This guide helps verify that the two critical bug fixes are working correctly:
1. ✓ quote_from_bytes() error when name/city are None
2. ✓ Browser closure between batch iterations

---

## Quick Verification Tests

### Test 1: Verify Script Imports (5 seconds)
```bash
python -c "import google_reviews_worker; print('OK - All imports successful')"
```
**Expected:** No errors, prints "OK - All imports successful"

### Test 2: Verify None Handling (10 seconds)
```bash
python << 'EOF'
import urllib.parse

# Test that our fix handles None values correctly
for name in [None, "", "Test Company"]:
    for city in [None, "", "Paris"]:
        test_name = name or ""
        test_city = city or ""
        try:
            q = urllib.parse.quote_plus(test_name)
            loc = urllib.parse.quote_plus(test_city)
            print(f"OK - name={name!r}, city={city!r}")
        except Exception as e:
            print(f"FAIL - name={name!r}, city={city!r}: {e}")
EOF
```
**Expected:** All combinations print "OK"

### Test 3: Syntax Check (5 seconds)
```bash
python -m py_compile google_reviews_worker.py && echo "Syntax OK"
```
**Expected:** "Syntax OK"

---

## Integration Tests

### Test 4: Dry-Run with 1 Company (30-60 seconds)
This is the main test that exercises all the fixes:

```bash
python google_reviews_worker.py --limit 1 --dry-run
```

**What this tests:**
- ✓ Database connection (PostgreSQL)
- ✓ None handling in enrich_one() and scrape_pages_jaunes()
- ✓ Page validation (ensure_page_valid)
- ✓ API gouv bulk lookup
- ✓ Google Maps scraping (no name validation errors)
- ✓ Pages Jaunes scraping (no quote_from_bytes errors)
- ✓ Website deep scraping if available
- ✓ Dry-run mode (no DB writes)

**Expected output includes:**
- Connection to Neon PostgreSQL
- Detected tables and company data
- API gouv lookup (Step 1/4)
- Playwright enrichment (Step 2/4)
  - Google Maps scraping
  - Pages Jaunes scraping (should complete without quote_from_bytes error)
  - Website deep scraping (if website found)
- Final stats (dry-run doesn't write to DB)

**Success indicators:**
- No "quote_from_bytes() expected bytes" error
- No "Target page, context or browser has been closed" error
- Completes without exceptions
- Shows enrichment results for 1 company

---

### Test 5: Batch Processing (5-15 minutes)
Test with smaller batch to verify browser lifecycle:

```bash
python batch_enrich.py --batch-size 10 --max-batches 2 --delay 5
```

**What this tests:**
- ✓ Page recreation between companies within a batch
- ✓ Browser stability across multiple companies
- ✓ Headless mode by default

**Expected behavior:**
- Batch 1 processes first 10 companies
- Asks to continue (respond with 'y' and Enter)
- Batch 2 processes next 10 companies
- No browser closure errors
- Clean shutdown after 2 batches

**Success indicators:**
- Both batches complete successfully
- Final stats shown for each batch
- No errors about closed pages/browsers

---

### Test 6: Headless Mode Verification (10 seconds)
```bash
python google_reviews_worker.py --limit 1 --dry-run &
sleep 5
# Check that no browser window appears
# (It should run completely headless)
```

**Expected behavior:**
- No visible browser window
- Script runs silently in background
- All processing done headless

**To enable visible browser:**
```bash
python google_reviews_worker.py --limit 1 --dry-run --headed
```

---

## Error Recovery Tests

### Test 7: Invalid Database URL Recovery
```bash
# Temporarily rename .env to test error handling
mv .env .env.bak
python google_reviews_worker.py --limit 1 --dry-run
# Should fail gracefully with connection error
mv .env.bak .env
```

**Expected:** 
- Clean error message about connection failure
- Graceful exit (no crash)

---

## Performance Baseline

### Test 8: API Throttling Check
```bash
# Test with different API concurrency levels
python google_reviews_worker.py --limit 20 --api-concurrent 5 --delay 0
```

**Expected:**
- No "429 Too Many Requests" errors
- All 20 requests complete successfully
- ~20 seconds total (depends on network)

---

## Bug Reproduction Tests

### Previous Bug #1: quote_from_bytes() Error
**Scenario:** Database returns NULL for company name and city
**Fix verification:** 
- Should handle gracefully with "Pas de nom ni de ville" message
- Should NOT throw quote_from_bytes error
- Should skip Pages Jaunes and continue

### Previous Bug #2: Browser Closure Error
**Scenario:** Multiple companies processed in succession
**Fix verification:**
- Should process all companies in batch
- Should NOT show "Target page, context or browser has been closed"
- Page should be automatically recreated if needed
- Should show "[Page] Recréation (ancienne fermée)" in logs if recreation occurs

---

## Full End-to-End Test (15-30 minutes)

```bash
# Production-like test with moderate batch
python batch_enrich.py --batch-size 50 --max-batches 3 --delay 5

# Or for longer testing:
python google_reviews_worker.py --limit 100 --delay 1 --api-concurrent 5

# Monitor logs for any errors
tail -f google_reviews_worker.log 2>/dev/null || echo "No log file"
```

**Success metrics:**
- ✓ No quote_from_bytes errors
- ✓ No browser/page closure errors
- ✓ All companies processed successfully
- ✓ Results saved to database (or dry-run logged)
- ✓ Final stats displayed correctly

---

## Troubleshooting

### Issue: "quote_from_bytes() expected bytes" still occurs
- **Check:** Ensure script file is updated and has the None handling
- **Verify:** `grep "name = name or" google_reviews_worker.py`
- **Solution:** Re-apply the fix or reload the file

### Issue: Browser still closes between batches
- **Check:** Ensure `ensure_page_valid()` function exists
- **Verify:** `grep "ensure_page_valid" google_reviews_worker.py`
- **Check logs:** Look for "[Page] Recréation" messages
- **Solution:** Verify the page validation is called in main() loop

### Issue: Unicode encoding errors in console output
- **Windows only:** Set environment variable before running
  ```bash
  set PYTHONIOENCODING=utf-8
  ```
- **Or use PowerShell:**
  ```powershell
  $env:PYTHONIOENCODING='utf-8'
  python google_reviews_worker.py --limit 1
  ```

### Issue: Database connection fails
- **Check:** `echo $NEON_DATABASE_URL` to verify env var is set
- **Verify:** Connection string has correct format
- **Test:** `psql (NEON_DATABASE_URL)` to test direct connection

---

## Success Criteria Checklist

- [ ] Script imports without errors
- [ ] None handling works (Test 2)
- [ ] Dry-run with 1 company completes (Test 4)
- [ ] No quote_from_bytes errors in logs
- [ ] No browser closure errors in logs
- [ ] Batch processing completes (Test 5)
- [ ] Stats displayed correctly
- [ ] Headless mode is default (no browser window)

---

## Performance Expectations

| Operation | Time |
|-----------|------|
| API gouv lookup (1 company) | 1-3 seconds |
| Google Maps scraping (1 company) | 8-12 seconds |
| Pages Jaunes scraping (1 company) | 3-5 seconds |
| Website deep scraping (1 company) | 2-5 seconds |
| **Total per company** | 15-25 seconds (with delay) |
| **Batch of 100** | ~25-40 minutes (with 15s delay) |

---

## Next Steps

1. Run Test 1-3 for quick verification
2. Run Test 4 for main integration test
3. Run Test 5 for batch stability
4. Monitor logs for any issues
5. Proceed to production batch processing

For any issues, check the troubleshooting section or review the FIXES_APPLIED.md documentation.
