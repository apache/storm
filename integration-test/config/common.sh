USER_SCRIPT="user-script.sh"
[[ -f $USER_SCRIPT ]] && echo "Running ${USER_SCRIPT}" && bash ${USER_SCRIPT} || echo "${USER_SCRIPT} not found/executed, continuing."
#apt-get update
#apt-get --yes remove openjdk-6-jre-headless
#apt-get --yes install openjdk-7-jdk
