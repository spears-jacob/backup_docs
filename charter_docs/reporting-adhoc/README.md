# Reporting Adhoc

Reporting adhoc has adhoc ask and query.
# This is how you create a merge Conflicts

# Folder structures

# For the merge conflicts

Here's the folder structure to help you navigate

```
adhoc-query
├── readme (attach email, jira ticket as a description of the request)
├── query.hql (query)
├── other-related.file (any other related file)
```

# List of regular reporting metrics

| Type | Name | Short Description | Link to sql |  
| ---- | ---- | ----------------- | ---- |
| error metric | errors | Errors related reporting metrics | [here](https://gitlab.spectrumxg.com/product-intelligence/reporting-adhoc/tree/master/spectrum-tv/errors)|
| login metric | login | Login related reporting metrics | [here](https://gitlab.spectrumxg.com/product-intelligence/reporting-adhoc/tree/master/spectrum-tv/login)|
| playback metric | playback | Playback related regular reporting metrics | [here](https://gitlab.spectrumxg.com/product-intelligence/reporting-adhoc/tree/master/spectrum-tv/playback) |
| unique metric | unique | Unique counts related regular reporting metrics | [here](https://gitlab.spectrumxg.com/product-intelligence/reporting-adhoc/tree/master/spectrum-tv/unique) |


# List of adhoc requests

| Type | Name | Short Description | Link |
| ---- | ---- | ----------------- | ---- |
| concurrent streams | max-concurrent | Max number of concurrent streams of a day | [here](https://gitlab.spectrumxg.com/product-intelligence/reporting-adhoc/tree/master/spectrum-tv/max-concurrent) |
| network status | network-noswitch | Number of mobile users never switch between in-home or out of home | [here](https://gitlab.spectrumxg.com/product-intelligence/reporting-adhoc/tree/master/spectrum-tv/network-noswitch) |
| network status | network-switch | Number of mobile ever switched to out of home and days out of home| [here](https://gitlab.spectrumxg.com/product-intelligence/reporting-adhoc/tree/master/spectrum-tv/network-switch) |
| bitrate histogram | bitrate-histogram | Histogram of bitrates group by connection type, isp, application type and version| [here](https://gitlab.spectrumxg.com/product-intelligence/reporting-adhoc/tree/master/spectrum-tv/bitrate-histogram) |

# Grouping Level and Grouping ID

`prod.venona_daily_set_agg` and `prod.venona_weekly_set_agg` table are populated with grouping sets. Here's a look up table for grouping set and grouping id. Click [here](https://gitlab.spectrumxg.com/product-intelligence/reporting-adhoc/wikis/grouping-id-lookup) for the link on wiki.

# Example of readme

JIRA: Link to jira ticket if any

From: where the request fro,

Ask: what's the ask.

Steps to replicate (if more than just query.hql in there):
```sh
Rscript all.R # on local
sh run.sh # on local
hive -f query.hql # on gv
```
