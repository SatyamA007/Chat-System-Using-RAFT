state = ['Follower', 'Candidate', 'Leader']

nodos = {
    '1': {'name': 'a', 'port': 5001},
    '2': {'name': 'b', 'port': 5002},
    '3': {'name': 'c', 'port': 5003},
    '4': {'name': 'd', 'port': 5004},
    '5': {'name': 'e', 'port': 5005},
}

interface = {'port': 7004}

heartbeatInterval = 2
DEBUGGING_ON = False
GROUP_KEY_NBITS = 256 # for 21 byte messages
NODE_KEY_NBITS = 2048
MAX_RETRIES = 5

'''
Sample log entries

{'term': 13, 'type': 'createGroup', 'group_id': 'g1', 'client_ids': ['2', '1'], 'group_public_key': PublicKey(86702799256623328332783554034575463517726992144676035689201744930242486124477, 65537), 'private_key_encrypted': [b'(\xfd\xb5\xd0\xde\xe1r\xdd\x10N\xf3\t\xb7\xea1\xab\xc2Hw!U\xab\xe6qd\x9d6\x88\xdd*\x1dFv"\x13\xfbg\xa5,\x87^\xc8\xcb\xf18\xe8/\x90\xc7\xad\xb8Q\x1f\xc3\xef\x17\x87Vj\x08\xfb\x82\xf4\x8aL\x99\xbaX\x81\xcb\x1b\x87_\x9b\xa4\xcas\x15\xa9\x0c\x17\x01\x00D\\LO\xbf\xb0\x1a\x93gv-\x89\x9fgI\x9a%\xb5\xb5\xb6\xeb\xab\xc4T:q\x00\x01\xc8\x96\xd0\xf2\xfa\x9c\x0e\x10p\xdd\xe3\xed}\x08\x98Z\xfe\xaen\x10\xeb\xf0 \xafM\x89\xf9\xdd\x1e\x8e\x93X\xf1S\xb9\x17\xd0O\xba\r\x03\xeddL\xf1)C=|\xd8\t\xd8\xdczS\x1dDJ\x94\xb9l\xb6\xd6N\xbc\x9d\xaf\x1f\x16&\xd1\x9b\x7f\'\x06x.\xbc|\xe7\xf5\xdbn\xc7\xd4x\'w\\\xf4 Wrg;,f\xa0(\xaaS\x82\xa6\xda\x02\x01\xca\n\x87]K\xa3\x0b\x08\x98\xc5\xfe\xe5\x89b\xcd\x14\x96cF\xa2\xb2\xee\x93\x83h\x9c,\xefw\xcbi\xb2rV\xbe,\xd1\x8b\x87', b'\x00\x02\x82\x99\xccqK\x9f]\xd5\x0c\x83\x8ar\xc1\x8c0up\xb9F^\x93\xeb\x90\xaeTz\xbb\xd0^\xf4\x87/\rq\x8f\xe6\xef\x85\x87\x00\x80\x04\x95\xcb\x00\x00\x00\x00\x00\x00\x00\x8c\x07rsa.key\x94\x8c\nPrivateKey\x94\x93\x94)\x81\x94(\x8a!\xbd\xbf\xf8\x12ln\xd9$\x16!\xba\x0e6\x18\xc8\xb8\xda1\x88\x0c\xed7\x9c\xb78uG\xd8\x9a\x0b\xb0\xbf\x00J\x01\x00\x01\x00\x8a!\x193+\x11\xed57T?H\xa5<\xad\xb8I\xe6EF\xbb\x87\x83r$Bq\xc2\x80/\x17W\x04\xa9\x00\x8a\x12\x9f*6\xcb\xa8\xb3\xaa\xa3\xd0\x91C\xf0\xb6\xa5\t\x9b\xde\x00\x8a\x10#\x94\xf3[4\xa5\xe9pj\x8c\x86\x03\xa2q\xdc\x00\x8a\x12A\x7f\\#[i\x8aQ\xb6!\x9f\xabDa\xa0\xb6\x89\x00\x8a\x0f\r\xb1\xaa$\xf0=,t5\x81\x93\x80E\x122\x8a\x12\x947m8p\xf4\xa8\x1a\xbe\xa8\x87HM<\xb9\x8d\xd5\x00t\x94b.']}
{'term': 26, 'type': 'message', 'group_id': 'g1', 'client_ids': ['2', '1'], 'group_public_key': PublicKey(86702799256623328332783554034575463517726992144676035689201744930242486124477, 65537), 'sender': '1', 'message': b"\x14?\xa8\xb2\x91\x11\x1fH\xb4\xad3\x1dz9\x1ed{\x15'\xbd\x9bSg\x88\x9d&\xbb\xd4\xd5N\xffu"}
{'term': 10, 'type': 'message', 'group_id': 'g1', 'client_ids': ['2', '1'], 'group_public_key': PublicKey(86702799256623328332783554034575463517726992144676035689201744930242486124477, 65537), 'private_key_encrypted': [b'(\xfd\xb5\xd0\xde\xe1r\xdd\x10N\xf3\t\xb7\xea1\xab\xc2Hw!U\xab\xe6qd\x9d6\x88\xdd*\x1dFv"\x13\xfbg\xa5,\x87^\xc8\xcb\xf18\xe8/\x90\xc7\xad\xb8Q\x1f\xc3\xef\x17\x87Vj\x08\xfb\x82\xf4\x8aL\x99\xbaX\x81\xcb\x1b\x87_\x9b\xa4\xcas\x15\xa9\x0c\x17\x01\x00D\\LO\xbf\xb0\x1a\x93gv-\x89\x9fgI\x9a%\xb5\xb5\xb6\xeb\xab\xc4T:q\x00\x01\xc8\x96\xd0\xf2\xfa\x9c\x0e\x10p\xdd\xe3\xed}\x08\x98Z\xfe\xaen\x10\xeb\xf0 \xafM\x89\xf9\xdd\x1e\x8e\x93X\xf1S\xb9\x17\xd0O\xba\r\x03\xeddL\xf1)C=|\xd8\t\xd8\xdczS\x1dDJ\x94\xb9l\xb6\xd6N\xbc\x9d\xaf\x1f\x16&\xd1\x9b\x7f\'\x06x.\xbc|\xe7\xf5\xdbn\xc7\xd4x\'w\\\xf4 Wrg;,f\xa0(\xaaS\x82\xa6\xda\x02\x01\xca\n\x87]K\xa3\x0b\x08\x98\xc5\xfe\xe5\x89b\xcd\x14\x96cF\xa2\xb2\xee\x93\x83h\x9c,\xefw\xcbi\xb2rV\xbe,\xd1\x8b\x87', b'\x00\x02\x82\x99\xccqK\x9f]\xd5\x0c\x83\x8ar\xc1\x8c0up\xb9F^\x93\xeb\x90\xaeTz\xbb\xd0^\xf4\x87/\rq\x8f\xe6\xef\x85\x87\x00\x80\x04\x95\xcb\x00\x00\x00\x00\x00\x00\x00\x8c\x07rsa.key\x94\x8c\nPrivateKey\x94\x93\x94)\x81\x94(\x8a!\xbd\xbf\xf8\x12ln\xd9$\x16!\xba\x0e6\x18\xc8\xb8\xda1\x88\x0c\xed7\x9c\xb78uG\xd8\x9a\x0b\xb0\xbf\x00J\x01\x00\x01\x00\x8a!\x193+\x11\xed57T?H\xa5<\xad\xb8I\xe6EF\xbb\x87\x83r$Bq\xc2\x80/\x17W\x04\xa9\x00\x8a\x12\x9f*6\xcb\xa8\xb3\xaa\xa3\xd0\x91C\xf0\xb6\xa5\t\x9b\xde\x00\x8a\x10#\x94\xf3[4\xa5\xe9pj\x8c\x86\x03\xa2q\xdc\x00\x8a\x12A\x7f\\#[i\x8aQ\xb6!\x9f\xabDa\xa0\xb6\x89\x00\x8a\x0f\r\xb1\xaa$\xf0=,t5\x81\x93\x80E\x122\x8a\x12\x947m8p\xf4\xa8\x1a\xbe\xa8\x87HM<\xb9\x8d\xd5\x00t\x94b.'], 'sender': '3', 'message': b'\x9cK\xb5]\xe5\xc4\xfe \x80H}\x0b\xd7zp"\xcc\x90\xf5\xcaG\xaej\xc4vm;\xf1\x08\xb8\xda\x91'}
{'term': 10, 'type': 'kick', 'group_id': 'g1', 'client_ids': ['1'], 'group_public_key': PublicKey(85912157702520100861628898680809507987355910454463397303513212628548259725977, 65537), 'private_key_encrypted': [b'\x00\x02`L\xcf\xef\xee\xf7mh\x0e\xf7\x06\xb4\xd2R\x9a\xf6K\x02vC#\xaa\x03D\xa1\xa4\rq\xaak\xf9\xa1\x0en\x19r{\xa3k"\x00\x80\x04\x95\xca\x00\x00\x00\x00\x00\x00\x00\x8c\x07rsa.key\x94\x8c\nPrivateKey\x94\x93\x94)\x81\x94(\x8a!\x99\xc2\xe4s\x12\xb8m:\x91\xc4\x97Om\xa6\x01\xa7\xf1\xe3\xe6\x80*\x9d\n\xf9\x1e\xdb\xad\xb0\xdd\x8e\xf0\xbd\x00J\x01\x00\x01\x00\x8a!\xc1\xa0\xa2r\xdf\xf3\xf2V\xe2\xdf\x8c\xce\xd0S\xf3s!\x86|Yv\xce \xae_Y\xe4\x18`:\xcb\x94\x00\x8a\x12\x05!$\xacs\x84|\xb9\xdb\x81\x84\xfd\x10N&N\xef\x00\x8a\x10\x85\x1f\xa0\x9a\xf0S\x0e\xb4\xcd\xc9\xd8\xa9\xc40\xcb\x00\x8a\x12A\x0e\xd4>0\x05;\xc8\xf0\xa9\xb6]\xaeX\xde\xfa\xd0\x00\x8a\x0fI\xb1\xb2CN\xd7\xf2k\xb8\xa1-0\x80\xf6$\x8a\x11n\x88\x8d\xf7g\xef\x15\x02S\xef\x82\x8d\xb1\x06\x18oOt\x94b.']}
              
'''



import os.path
import pickle
import rsa

#for exporting new key to pem files
def save_new_key_pairs(client_id:str):
    def export_private_key(private_key, filename):
        with open( filename, "wb") as file:            
            privateKeyPkcs1PEM = pickle.dumps(private_key)
            file.write(privateKeyPkcs1PEM)
            file.close()
    def export_public_key(public_key, filename):
        with open(filename, "wb") as file:
            publicKeyPkcs1PEM = pickle.dumps(public_key)
            file.write(publicKeyPkcs1PEM)
            file.close()
    if not os.path.exists("keys"):
        os.makedirs("keys")
    file_exists = os.path.exists("keys/" + 'private_key_'+client_id+'.pem') and os.path.exists("keys/" + 'public_key_'+client_id+'.pem')
    
    if file_exists:
        print("Keys already exists for"+client_id)
        return

    publicKey, privateKey = rsa.newkeys(NODE_KEY_NBITS) #, poolsize=4, exponent=1) 
    cwd = os.getcwd()

    print('Exporting keys.')
    export_private_key(privateKey,   cwd+"/keys/" + 'private_key_'+client_id+'.pem')
    export_public_key(publicKey, cwd+"/keys/" + 'public_key_'+client_id+'.pem')

#for importing existing key from pem files
def fetch_key_pairs(client_id:str):
    cwd = os.getcwd()
    file_exists = os.path.exists(cwd+"/keys/" + 'private_key_'+client_id+'.pem') and os.path.exists(cwd+"/keys/" + 'public_key_'+client_id+'.pem')
    
    if not file_exists:
        print("Keys not found for "+client_id+" ....generating new pair")
        save_new_key_pairs(client_id)

    public_key_string = open(cwd+"/keys/" + "public_key_"+client_id+".pem","rb").read()
    publicKeyReloaded = pickle.loads(public_key_string)
    print('Public key: ', publicKeyReloaded)
    private_key_string = open(cwd+"/keys/" + "private_key_"+client_id+".pem","rb").read()
    privateKeyReloaded = pickle.loads(private_key_string)
    print('Private Key: ', privateKeyReloaded)
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
    group_key_PEM = pickle.dumps(group_key)
    group_key_encrypted = rsa.encrypt(group_key_PEM, client_public_key)
    return group_key_encrypted

def decrypt_group_key(group_key, client_id):
    _,client_private_key = fetch_key_pairs(client_id)
    try:
        group_privateKey_PEM = rsa.decrypt(group_key, client_private_key)
        group_privateKey = pickle.loads(group_privateKey_PEM) 

    except Exception as e:
        print('decrypt_group_key error: ',e)
        return ''
    return group_privateKey

def decrypt_message(msg, group_key):
    try:
        msg = rsa.decrypt(msg, group_key)
        msg = msg.decode('utf8')
    except Exception as e:
        print('decrypt_message error:', e)
        return ''
    return msg

