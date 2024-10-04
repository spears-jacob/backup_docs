.
| {
  meta: .data[],
  info: ((.data | to_entries[]) as $data | {date: .dates, position: $data.value.positions, delta: $data.value.deltas} | [.position, .delta, .date] | transpose[])
  }
| [
  .meta.product_id,
  .meta.country,
  .meta.category.store,
  .meta.category.store_id,
  .meta.category.device,
  .meta.category.device_id,
  .meta.category.id,
  .meta.category.name,
  .meta.category.subtype,
  .info[]
  ]
| @csv
