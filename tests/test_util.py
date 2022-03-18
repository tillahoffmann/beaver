import beaver_build as bb


def test_crc32():
    crc32 = bb.Crc32(b"hello ")
    crc32.update(b"world")
    # Cf. https://emn178.github.io/online-tools/crc32.html.
    assert crc32.hexdigest() == "0d4a1185"
