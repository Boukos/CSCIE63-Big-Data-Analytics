REM purge contents
curl.exe  -i -H accept:application/json -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d "{\"statements\":[{\"statement\":\"MATCH (m) detach delete m\"}]}"
