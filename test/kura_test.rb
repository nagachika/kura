require 'test_helper'
require "tempfile"

class KuraTest < Test::Unit::TestCase
  def test_that_it_has_a_version_number
    refute_nil ::Kura::VERSION
  end

  def assert_private_key_equal(key1, key2)
    assert_equal(key1.n, key2.n)
    assert_equal(key1.d, key2.d)
    assert_equal(key1.p, key2.p)
    assert_equal(key1.q, key2.q)
    assert_equal(key1.dmp1, key2.dmp1)
    assert_equal(key1.dmq1, key2.dmq1)
    assert_equal(key1.iqmp, key2.iqmp)
  end

  def test_get_private_key_from_string
    rsa = OpenSSL::PKey::RSA.generate(2048)
    str = rsa.to_pem

    assert_private_key_equal(rsa, Kura.get_private_key(str))
  end

  def test_get_private_key_from_file
    rsa = OpenSSL::PKey::RSA.generate(2048)
    str = rsa.to_pem
    Tempfile.open("kura-test") do |f|
      f.puts(str)
      f.close

      assert_private_key_equal(rsa, Kura.get_private_key(f.path))
    end
  end

  def test_get_private_key_from_pathname
    rsa = OpenSSL::PKey::RSA.generate(2048)
    str = rsa.to_pem
    Tempfile.open("kura-test") do |f|
      f.puts(str)
      f.close

      assert_private_key_equal(rsa, Kura.get_private_key(Pathname.new(f.path)))
    end
  end
end
