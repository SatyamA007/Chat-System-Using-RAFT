state = ['Follower', 'Candidate', 'Leader']

nodos = {
    '1': {'name': 'a', 'port': 5001},
    '2': {'name': 'b', 'port': 5002},
    '3': {'name': 'c', 'port': 5003},
}

interface = {'port': 5004}

heartbeatInterval = 2
debugging_on = False

import os.path
import rsa

#for exporting new key to pem files
def save_new_key_pairs(client_id:str):
    def export_private_key(private_key, filename):
        with open( filename, "wb") as file:            
            privateKeyPkcs1PEM = private_key.save_pkcs1()
            file.write(privateKeyPkcs1PEM)
            file.close()
    def export_public_key(public_key, filename):
        with open(filename, "wb") as file:
            publicKeyPkcs1PEM = public_key.save_pkcs1()
            file.write(publicKeyPkcs1PEM)
            file.close()
    file_exists = os.path.exists("keys/" + 'private_key_'+client_id+'.pem') and os.path.exists("keys/" + 'public_key_'+client_id+'.pem')
    
    if file_exists:
        print("Keys already exits for"+client_id)
        return

    publicKey, privateKey = rsa.newkeys(1024) 
    cwd = os.getcwd()
    print(cwd)

    export_private_key(privateKey,   cwd+"/keys/" + 'private_key_'+client_id+'.pem')
    export_public_key(publicKey, cwd+"/keys/" + 'public_key_'+client_id+'.pem')

#for importing existing key from pem files
def fetch_key_pairs(client_id:str):
    cwd = os.getcwd()
    file_exists = os.path.exists(cwd+"/keys/" + 'private_key_'+client_id+'.pem') and os.path.exists(cwd+"/keys/" + 'public_key_'+client_id+'.pem')
    
    if not file_exists:
        print("Keys not found for "+client_id+" ....generating new pair")
        save_new_key_pairs(client_id)

    public_key_string = open(cwd+"/keys/" + "public_key_"+client_id+".pem","r").read()
    publicKeyReloaded = rsa.PublicKey.load_pkcs1(public_key_string.encode('utf-8')) 

    private_key_string = open(cwd+"/keys/" + "private_key_"+client_id+".pem","r").read()
    privateKeyReloaded = rsa.PrivateKey.load_pkcs1(private_key_string.encode('utf-8')) 

    #Example for reference
    '''
    plaintext = "vinay kumar shukla".encode('utf-8')
    print("Plaintext: ", plaintext)

    ciphertext = rsa.encrypt(plaintext, publicKeyReloaded)
    print("Ciphertext: ", ciphertext)
    
    decryptedMessage = rsa.decrypt(ciphertext, privateKeyReloaded)
    print("Decrypted message: ", decryptedMessage)
    '''

    return publicKeyReloaded, privateKeyReloaded

def encrypt_group_key(client_public_key, group_key):
    group_key_PEM = group_key.save_pkcs1()
    group_key_encrypted = rsa.encrypt(group_key_PEM, client_public_key)
    return group_key_encrypted

def decrypt_group_key(group_key, client_id):
    _,client_private_key = fetch_key_pairs(client_id)
    try:
        group_privateKey_PEM = rsa.decrypt(group_key, client_private_key)
        group_privateKey = rsa.PrivateKey.load_pkcs1(group_privateKey_PEM) 
    except Exception as e:
        print(e)
        return ''
    return group_privateKey

