tsm login -s https://prtableau.e-470.com -u jspears@e-470.com

#tabcmd login -s https://prod-useast-b.online.tableau.com -t mysite -u authority@email.com -p user


tabcmd login -s https://online.tableau.com -t https://prtableau.e-470.com/#/home -u jspears@e-470.com -p 


tabcmd login --server https://online.tableau.com --site https://prtableau.e-470.com/#/home --username jspears --password Obsidian478478!
tabcmd listsites



if [ $# -lt 3 ] || [ $# -gt 3 ] || [ -z "${1}" ] || [ -z "${2}" ] || [ -z "${3}" ]; then
    echo "

    Usage: tab_email.sh <tabview> <email> <user> 

    The shell script will conclude if these requirements are not met.
    The Tableau Worksheet web address, email, and user must match values found in project.properties.

      "
    exit 1
fi

# assigns input variables
email=${1}
tabview=${2}
user=${3}

grep --quiet tabview= project.properties
is_tabview_present=$?
grep --quiet email= project.properties
is_email_present=$?
grep --quiet user= project.properties
is_user_present=$?

if [ ! -f project.properties ] ; then
    echo "

    project.properties was not found.

    Please prepare one with comma delimited entries for Tableau Worksheet web address, email, and user. "

    exit 1
fi

if [ $is_email_present -ne 0 ] || [ $is_tabview_present -ne 0 ] || [ $is_user_present -ne 0 ]; then
    echo "

    project.properties was not found to have all the requisite entries.

    Please prepare it with comma delimited entries for Tableau Worksheet web address, email, and user. "

    exit 1
fi

# parses out lists of variables
export email_cdl=$(perl -nle'print $& while m{(?<=email=).*}g' rosetta.properties);
export tabview_cdl=$(perl -nle'print $& while m{(?<=tabview=).*}g' rosetta.properties);
export user_cdl=$(perl -nle'print $& while m{(?<=user=).*}g' rosetta.properties);

# puts lists into arrays
IFS=', ' read -r -a email_array <<< "$email_cdl"
IFS=', ' read -r -a tabview_array <<< "$tabview_cdl"
IFS=', ' read -r -a user_array <<< "$user_cdl"

# tests that email input matches
if ( ! printf '%s\n' ${email_array[@]} | grep -q "^$email$" ) ; then
  echo "
  The email entry in rosetta.properties did not match what was entered ($email).

  Please use a email in the following list or revise rosetta.properties.

  ${email_array[@]}

  "
  exit 1
fi

# tests that project title input matches
if ( ! printf '%s\n' ${tabview_array[@]} | grep -q "^$tabview$" ) ; then
  echo "
  The tabview entry in rosetta.properties did not match what was entered ($tabview).

  Please use a tabview in the following list or revise rosetta.properties.

  ${tabview_array[@]}

  "
  exit 1
fi

# tests that user input matches
if ( ! printf '%s\n' ${user_array[@]} | grep -q "^$user$" ) ; then
  echo "
  The user entry in rosetta.properties did not match what was entered ($user).

  Please use a user in the following list or revise rosetta.properties.

  ${user_array[@]}

  "
  exit 1
fi

# Assigns secret password variable from credentials.json
username=$(echo $json | jq -r '.profiles[] | select(.user=="'${user}'") | '.user'' )
echo $username
password=$(echo $json | jq -r '.profiles[] | select(.user=="'${user}'") | '.pwd'' )
echo $password

