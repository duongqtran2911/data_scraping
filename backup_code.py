def clear_cache(driver):
    driver.execute_script("window.localStorage.clear();")
    driver.execute_script("window.sessionStorage.clear();")
    driver.execute_script("caches.keys().then(function(keys) { keys.forEach(function(key) { caches.delete(key); }); });")