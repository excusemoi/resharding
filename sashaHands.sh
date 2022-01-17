SHARDS=(adult appliances autoproduct bags bathroom beach beauty beauty1 beauty2 beauty3 beauty4 beauty5 beauty6 bijouterie blazers_wamuses bl_shirts books books_archeology books_children books_fiction business carnival children children_boys children_girls children_shoes children_things costumes creativity dresses electronic1 electronic2 electronic3 electronic4 electronic5 electronic6 garden garden1 gift1 gift2 gift3 gift4 hand_accessories hats_gloves_scarves housecraft housecraft2 interior interior1 interior2 interior3 interior4 interior_root jeans jewellery jumpers_cardigans kitchen kitchen1 kitchen2 kitchen3 kitchen4 kitchen_root longsleeves men_clothes men_mixtape men_shoes moms office outwear overalls pants product religion rollnecks rooms school shealth shoes shoes_accessories shorts short_tall skirts stationery sweatshirts_hoodies tops_tshirts toys travelling vests wedding women_bigsize women_shoes women_underwear1 women_underwear2 work_clothes zoo)
SHARD=sda
for i in "${SHARDS[@]}"
do
    SHARD="${i//_/-}"
    curl -s "http://catalog-backend-${SHARD}.wbx-ru.svc.k8s.dataline/internal/subjects/full/v1" --header 'X-Catalog-Client-Id: wbx_search' --header 'X-Catalog-Client-Secret: d0e8c0f2-1d9b852b5ae7d2-d38a48' | jq '.'
    echo "$SHARD"
   
done