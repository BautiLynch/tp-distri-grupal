import zlib

def get_hash_key(key, num_buckets):
    return zlib.adler32(key.encode()) % num_buckets

def routing_key(prefix, index, amount):
    return f"{prefix}_{index % amount}"

def routing_keys(prefix, amount):
    return [f"{prefix}_{index}" for index in range(amount)]
