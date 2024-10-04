.reviews[]
| [
    .author, (.title | gsub("[\\n\\t]"; "")), (.review | gsub("[\\n\\t]"; "")), (.original_title | gsub("[\\n\\t]"; "")), (.original_review | gsub("[\\n\\t]"; "")), .stars, .iso, .version, .date, .product_id, .product_name, .vendor_id, .store, .weight, .id, (.predicted_langs | join(","))
  ]
| @csv
