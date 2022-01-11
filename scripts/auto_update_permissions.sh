#!/bin/bash

source ./aws_env.sh

OPTION=$1

if [[ "$OPTION" != "--add-rule" && "$OPTION" != "--del-rule" ]]; then

  echo " - ERR: INVALID option specified."
  echo "        Valid Options: --add-rule, --del-rule"
  exit 1
fi

add_ingress_rule () {

  echo " - INFO: Adding 'ingress' rule for '$CURRENT_PUBLIC_IP' .. "
  aws ec2 authorize-security-group-ingress --group-id sg-4e94aa35 --protocol tcp --port 3306 --cidr $CURRENT_PUBLIC_IP/32

  if [[ $? -gt 0 ]]; then

    echo " - ERR: Proble adding the rule to the SecurityGroup"
  else
    echo " - SUCCESS: Added ingress rule."
  fi
}

del_ingress_rule () {

  echo " - INFO: Removing 'ingress' rule for '$CURRENT_PUBLIC_IP' .. "
  aws ec2 revoke-security-group-ingress --group-id sg-4e94aa35 --protocol tcp --port 3306 --cidr $CURRENT_PUBLIC_IP/32

  if [[ $? -gt 0 ]]; then

    echo " - ERR: Problem removing the rule to the SecurityGroup"
  else
    echo " - SUCCESS: Removed ingress rule."
  fi
}


INSTANCE_ID=$(curl --silent http://169.254.169.254/1.0/meta-data/instance-id)
echo " - INFO: Updating the entry for '$INSTANCE_ID' .. "

CURRENT_PUBLIC_IP=$(curl --silent http://169.254.169.254/latest/meta-data/public-ipv4)
echo "   PUBLIC_IP: $CURRENT_PUBLIC_IP"

if [[ "$OPTION" == "--add-rule" ]]; then

  add_ingress_rule
elif [[ "$OPTION" == "--del-rule" ]]; then

  del_ingress_rule
fi
