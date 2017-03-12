REM upload contents
curl.exe  -i -H accept:application/json -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d @statements.json

REM verify contents
curl.exe  -i -H accept:application/json -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d "{\"statements\":[{\"statement\":\"MATCH (m) return m\"}]}"
