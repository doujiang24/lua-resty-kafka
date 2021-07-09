#!/usr/bin/env bash

set -eu

KEYSTORE_FILENAME="kafka.keystore.jks"
CLIENT_KEYSTORE_FILENAME="client.keystore.jks"
# 100 years~
VALIDITY_IN_DAYS=36500
DEFAULT_TRUSTSTORE_FILENAME="kafka.truststore.jks"
TRUSTSTORE_WORKING_DIRECTORY="truststore"
KEYSTORE_WORKING_DIRECTORY="keystore"
CA_CERT_FILE="ca-cert"
KEYSTORE_SIGN_REQUEST="cert-file"
KEYSTORE_SIGN_REQUEST_SRL="ca-cert.srl"
KEYSTORE_SIGNED_CERT="cert-signed"

COUNTRY="DE"
STATE="BY"
OU="FOO"
CN_CLIENT="client"
CN="broker"
LOCATION="NUE"
PASS="confluent"

function file_exists_and_exit() {
  echo "'$1' cannot exist. Move or delete it before"
  echo "re-running this script."
  exit 1
}

if [ -e "$KEYSTORE_WORKING_DIRECTORY" ]; then
  file_exists_and_exit $KEYSTORE_WORKING_DIRECTORY
fi

if [ -e "$CA_CERT_FILE" ]; then
  file_exists_and_exit $CA_CERT_FILE
fi

if [ -e "$KEYSTORE_SIGN_REQUEST" ]; then
  file_exists_and_exit $KEYSTORE_SIGN_REQUEST
fi

if [ -e "$KEYSTORE_SIGN_REQUEST_SRL" ]; then
  file_exists_and_exit $KEYSTORE_SIGN_REQUEST_SRL
fi

if [ -e "$KEYSTORE_SIGNED_CERT" ]; then
  file_exists_and_exit $KEYSTORE_SIGNED_CERT
fi

echo "Welcome to the Kafka SSL keystore and trust store generator script."

trust_store_file=""
trust_store_private_key_file=""

  if [ -e "$TRUSTSTORE_WORKING_DIRECTORY" ]; then
    file_exists_and_exit $TRUSTSTORE_WORKING_DIRECTORY
  fi

  mkdir $TRUSTSTORE_WORKING_DIRECTORY
  echo
  echo "OK, we'll generate a trust store and associated private key."
  echo
  echo "First, the private key."
  echo

  openssl req -new -x509 -keyout $TRUSTSTORE_WORKING_DIRECTORY/ca-key \
    -out $TRUSTSTORE_WORKING_DIRECTORY/ca-cert -days $VALIDITY_IN_DAYS -nodes \
    -subj "/C=$COUNTRY/ST=$STATE/L=$LOCATION/O=$OU/CN=$CN"

  trust_store_private_key_file="$TRUSTSTORE_WORKING_DIRECTORY/ca-key"

  echo
  echo "Two files were created:"
  echo " - $TRUSTSTORE_WORKING_DIRECTORY/ca-key -- the private key used later to"
  echo "   sign certificates"
  echo " - $TRUSTSTORE_WORKING_DIRECTORY/ca-cert -- the certificate that will be"
  echo "   stored in the trust store in a moment and serve as the certificate"
  echo "   authority (CA). Once this certificate has been stored in the trust"
  echo "   store, it will be deleted. It can be retrieved from the trust store via:"
  echo "   $ keytool -keystore <trust-store-file> -export -alias CARoot -rfc"

  echo
  echo "Now the trust store will be generated from the certificate."
  echo

  keytool -keystore $TRUSTSTORE_WORKING_DIRECTORY/$DEFAULT_TRUSTSTORE_FILENAME \
    -alias CARoot -import -file $TRUSTSTORE_WORKING_DIRECTORY/ca-cert \
    -noprompt -dname "C=$COUNTRY, ST=$STATE, L=$LOCATION, O=$OU, CN=$CN" -keypass $PASS -storepass $PASS \
    -storetype pkcs12

  trust_store_file="$TRUSTSTORE_WORKING_DIRECTORY/$DEFAULT_TRUSTSTORE_FILENAME"

  echo
  echo "$TRUSTSTORE_WORKING_DIRECTORY/$DEFAULT_TRUSTSTORE_FILENAME was created."

  # don't need the cert because it's in the trust store.
  # PATCH: I do
  # rm $TRUSTSTORE_WORKING_DIRECTORY/$CA_CERT_FILE

echo
echo "Continuing with:"
echo " - trust store file:        $trust_store_file"
echo " - trust store private key: $trust_store_private_key_file"

mkdir $KEYSTORE_WORKING_DIRECTORY

echo
echo "Now, a keystore will be generated. Each broker and logical client needs its own"
echo "keystore. "
echo "     NOTE: currently in Kafka, the Common Name (CN) does not need to be the FQDN of"
echo "           this host. However, at some point, this may change. As such, make the CN"
echo "           the FQDN. Some operating systems call the CN prompt 'first / last name'"

# To learn more about CNs and FQDNs, read:
# https://docs.oracle.com/javase/7/docs/api/javax/net/ssl/X509ExtendedTrustManager.html

keytool -keystore $KEYSTORE_WORKING_DIRECTORY/$KEYSTORE_FILENAME \
  -alias localhost -validity $VALIDITY_IN_DAYS -genkey -keyalg RSA \
   -noprompt -dname "C=$COUNTRY, ST=$STATE, L=$LOCATION, O=$OU, CN=$CN" -keypass $PASS -storepass $PASS \
   -storetype pkcs12

echo "Creating client keystore"

keytool -keystore $KEYSTORE_WORKING_DIRECTORY/$CLIENT_KEYSTORE_FILENAME \
  -alias localhost -validity $VALIDITY_IN_DAYS -genkey -keyalg RSA \
   -noprompt -dname "C=$COUNTRY, ST=$STATE, L=$LOCATION, O=$OU, CN=$CN_CLIENT" -keypass $PASS -storepass $PASS \
   -storetype pkcs12

for keystore in $KEYSTORE_FILENAME $CLIENT_KEYSTORE_FILENAME
do
  echo
  echo "'$KEYSTORE_WORKING_DIRECTORY/$keystore' now contains a key pair and a"
  echo "self-signed certificate."

  echo
  echo "Fetching the certificate from the trust store and storing in $CA_CERT_FILE."
  echo

  keytool -keystore $trust_store_file -export -alias CARoot -rfc -file $CA_CERT_FILE -keypass $PASS -storepass $PASS

  echo
  echo "Now a certificate signing request will be made to the keystore."
  echo
  keytool -keystore $KEYSTORE_WORKING_DIRECTORY/$keystore -alias localhost \
    -certreq -file $KEYSTORE_SIGN_REQUEST -keypass $PASS -storepass $PASS

  echo
  echo "Now the trust store's private key (CA) will sign the keystore's certificate."
  echo
  openssl x509 -req -CA $CA_CERT_FILE -CAkey $trust_store_private_key_file \
    -in $KEYSTORE_SIGN_REQUEST -out $KEYSTORE_SIGNED_CERT \
    -days $VALIDITY_IN_DAYS -CAcreateserial
  # creates $KEYSTORE_SIGN_REQUEST_SRL which is never used or needed.

  echo
  echo "Now the CA will be imported into the keystore."
  echo
  keytool -keystore $KEYSTORE_WORKING_DIRECTORY/$keystore -alias CARoot \
    -import -file $CA_CERT_FILE -keypass $PASS -storepass $PASS -noprompt
  rm $CA_CERT_FILE # delete the trust store cert because it's stored in the trust store.

  echo
  echo "Now the keystore's signed certificate will be imported back into the keystore."
  echo
  keytool -keystore $KEYSTORE_WORKING_DIRECTORY/$keystore -alias localhost -import \
    -file $KEYSTORE_SIGNED_CERT -keypass $PASS -storepass $PASS

  echo
  echo "All done!"
  echo
  echo "Deleting intermediate files. They are:"
  echo " - '$KEYSTORE_SIGN_REQUEST_SRL': CA serial number"
  echo " - '$KEYSTORE_SIGN_REQUEST': the keystore's certificate signing request"
  echo "   (that was fulfilled)"
  echo " - '$KEYSTORE_SIGNED_CERT': the keystore's certificate, signed by the CA, and stored back"
  echo "    into the keystore"

    rm $KEYSTORE_SIGN_REQUEST_SRL
    rm $KEYSTORE_SIGN_REQUEST
    rm $KEYSTORE_SIGNED_CERT


done

echo "Get a public and private key file to test the mTLS connection with"
openssl pkcs12 -in $KEYSTORE_WORKING_DIRECTORY/$CLIENT_KEYSTORE_FILENAME -out $KEYSTORE_WORKING_DIRECTORY/combined.key -passin pass:$PASS -passout pass:$PASS
echo "Only get the key"
openssl pkcs12 -info -in $KEYSTORE_WORKING_DIRECTORY/$CLIENT_KEYSTORE_FILENAME -nodes -nocerts -passin pass:$PASS -out $KEYSTORE_WORKING_DIRECTORY/privkey.key
echo "Only get the cert chain"
openssl pkcs12 -info -in $KEYSTORE_WORKING_DIRECTORY/$CLIENT_KEYSTORE_FILENAME -nodes -nokeys -passin pass:$PASS -out $KEYSTORE_WORKING_DIRECTORY/certchain.crt

