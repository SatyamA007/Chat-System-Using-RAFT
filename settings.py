state = ['Follower', 'Candidate', 'Leader']

nodos = {
    '1': {'name': 'a', 'port': 5001},
    '2': {'name': 'b', 'port': 5002},
    '3': {'name': 'c', 'port': 5003},
}

interface = {'port': 5004}

heartbeatInterval = 2
debugging_on = False

'''
Sample log entries

{'type': 'create', 'term': 1, 'group_id': 'g33', 'client_ids': ['2', '1'], 'group_public_key': PublicKey(40991, 65537), 'private_key_encrypted': [b"\xae/&@\xfc/T\x8a\xa3\xa1N\x14\xfan\xc6\xb0\xbd\x07\xf0\x1d\xb8\xd9\xe9\x14d\xd6x\xc1\xf3-\xd50\xffkQ\xa0:#T_\xb4\xbf6\xf2\xd6\xaa\xc9Sx\xc5\xfc\xad\x7f\xf6P\xc2\x16Q|1'\xcd\xe7,\x01\xa4\xeb[\xa0\x86H%\xc0\xac\n\xfb\xb7\x110\xb2\xba\xadL\xab\xd7\xfbI\x95\r\xdd\xf4\xd11\x8bR^\x15\xe0\xcdF\xd5W\x8f\xfa\xa7\xc0^p\xd6\xa6\r\xeb\xd6~\xf2U\xfb\xa9R\xf3\x84\x93\xcf\x1c\xafh\x19\xa3", b'O1\xfa_ \xba\xe0\xe8_\xdb~\x1c:\x81\x18\\.\x1er\xb5\xf1\xef\xc2\xc8\xdfo.\xe1=\x9c\xdf\xfcl\xe1\xf2#7Z\r\x93\xad"\x93\xdf-\x8c\x94\xdf\x9d\xe3^\xba\xabK\x10\xf8\xe6\xfb\xca\x06c\x9ee\x0c\x01z\xbeF\xdf\xe0\x7f\xbf\x02\xda\xa7\x11w\xb2)/)H\xdcK\x1cP2p\xd0\xbeW2\x1b\xf2\xa6h\xbe|\xb9\x11\xa0\xc4\xd4\x95\xbc\xd7r\x89\x12\x97,\xe3\xe84\x07I\x120\xb5\xaf\x87\xc3sy\x1d\\\xbb\xe5']}
{'type': 'add', 'term': 0, 'group_id': 'j2', 'client_ids': ['2', '1', '3'], 'group_public_key': PublicKey(35611, 65537), 'private_key_encrypted': [b'\x83\x13[\x07e\n\xc7\x18\xa6\xd1\x95\xe1Ug\xfa\x1c\x15\xdd\x83\xe5\xc1\x15vHt\x81\xa8\x95\xa8\xc8\xfc\xc1RY\x9aL\xc8\x01\x8e}\x91\xf2rX\xd8\x17U\x91\xa3~3\xc6G6_\x8b\x89\xba\xd1w\x1f\xa7Ba\x8a\xc0\x8d\xca\xda\xe6\x9f\x01\xe7\xb5\xff<\xc9u\xf0\xdd\x80\xf8\xe7*\x01\xc3\x11\xf5\xdb\x1e\x1c,m\xe6>znBH\xb7\x92u\x8f\x0eh wB\xc6\xc1\xa2\x9b\xd40*\xc26\xaf\x91=\xd8\xf9C\x00)5\xd3`', b'\x01\xcb\xac\xe8\x05R<rj\xf6#\xb9\xd7`\xd9\xd2\x07\x93\x94\x07\x7f**3J\xee\xa1\x1e)\x9au7\xf7\x1a\x8c\x1c\xf0)}8\xec\r\xde\xfdC\x9e"U\ty#\xa1U>\xed\xbb\xf2\xaa\x8f(\xb9\xdd\xa6\xd2\x15\xb3\x8a\xed\x19\xf8\xdb\xd7\xb6\xf9\xd5\x1a\xed\x85qNA\x93\xa2P%\xcb`56u\xe6\xb7\x1fp\xcf\xd9\x18B\xb0H7\xbfp\xd0\xc9L\xf2h\xda\x84\x88\xe5\x1c\xc6RS/H\xeba\xf3ml\xee:8\x15E', b':\xbc\xd4\xae\x9e\xcd\x0eY\xcb\xbdj\xdf\xc3\x95IH\x7fc,\xb2\xdb\x82\xe5z;Q\x98\x9d\xc8\xb6\xf3\xf4c\x8aTg\x9e\xe5t;\x04\xdb\xea7\x1b\xefs:\x9b\x14\xb8\xf4\x03n\x82\x90\x92\xb5!\xac\xde=\xe5\xdd\xd7\xc1\xe1I\xcf\x8dL\xee\x8c6\x80\xbeSXP\xe4\xb0o\x9b`\xdc\xcc\xec\xaf\x89.6b\x81Tdy\x0f\xaey5K\xf4\xddj\xae\xfe\xff\xff\xf7\xcbW\xf83\x07\x04?\xc1)\xc1\x08\x02=S\xfd\xd4\xd0/:']}

'''



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

def encrypt_group_key(client_id, group_key):
    client_public_key = fetch_key_pairs(client_id)[0] 
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

