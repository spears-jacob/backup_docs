#tabcmd login -s https://prtableau.e-470.com -u jspears@e-470.com -p Obsidian478478!


if [ $# -lt 2 ] || [ $# -gt 3 ] || [ -z "${1}" ] || [ -z "${2}" ] || [ -z "${3}" ]; then
    echo "

    Usage: tab_email.sh <tabview> <email> <user> 

    The shell script will conclude if these requirements are not met.
    The Tableau Worksheet web address, email, and user must match values found in project.properties.

      "
    return
    else 
echo 'that works!'
fi
