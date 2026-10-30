import hashlib

def get_hash_key(key, num_buckets):
    hash_bytes = hashlib.md5(key.encode()).digest()
    hash_value = int.from_bytes(hash_bytes, byteorder="big")
    return hash_value % num_buckets

def routing_key(prefix, index, amount):
    return f"{prefix}_{index % amount}"

def routing_keys(prefix, amount):
    return [f"{prefix}_{index}" for index in range(amount)]
