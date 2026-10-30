import hashlib

def get_hash_key(key, num_buckets):
    hash_bytes = hashlib.md5(key.encode()).digest()
    hash_value = int.from_bytes(hash_bytes, byteorder="big")
    return hash_value % num_buckets