import random
import string


def random_string(N):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=N))
