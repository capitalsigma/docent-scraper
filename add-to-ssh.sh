host="silverlode.uchicago.edu"
user="pscollins" 				# FIXME later

# FIXME to deal with other case
to_add=$(cat ~/.ssh/id_rsa.pub)

echo "Adding key to remote server. You'll be prompted for your password."
ssh $user@$host "mkdir -p ~/.ssh && echo $to_add >> ~/.ssh/authorized_keys && chmod -R 600 ~/.ssh"
echo "Done."
