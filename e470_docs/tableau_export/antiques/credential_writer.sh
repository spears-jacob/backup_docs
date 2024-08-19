json_str='{ "key1: "value1","key2": "value2" }'
echo $json_str

jq '. + { "key4": "value4" }' <<<"$json_str"

user= jivy
jq -n --arg v "$user" '{"profiles" ["user": $user}]}
{
    "profiles": [
        {
            "user": "",
            "pwd": ""
        }
    ]
}