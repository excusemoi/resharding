curl -s "http://catalog-backend-$1.wbx-ru.svc.k8s.dataline/internal/subjects/full/v1" --header 'X-Catalog-Client-Id: wbx_search' --header 'X-Catalog-Client-Secret: d0e8c0f2-1d9b852b5ae7d2-d38a48' | jq '.'
echo "$SHARD"