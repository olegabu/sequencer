#include <sequencer/sdk/client_signer.hpp>

#include <algorithm>
#include <functional>
#include <string>

#include <gtest/gtest.h>

namespace sequencer::sdk {
namespace {

Payload payloadOf(const std::string& s) {
  return Payload(reinterpret_cast<const std::byte*>(s.data()), s.size());
}

Ed25519PrivateKey testPrivateKey(std::byte seedByte = std::byte{0x07}) {
  Ed25519PrivateKey key{};
  key.fill(seedByte);
  return key;
}

Ed25519PublicKey derivePublicKey(const Ed25519PrivateKey& privateKey) {
  EVP_PKEY* key = EVP_PKEY_new_raw_private_key(EVP_PKEY_ED25519, nullptr,
                                                reinterpret_cast<const unsigned char*>(privateKey.data()),
                                                privateKey.size());
  Ed25519PublicKey publicKey{};
  std::size_t len = publicKey.size();
  EVP_PKEY_get_raw_public_key(key, reinterpret_cast<unsigned char*>(publicKey.data()), &len);
  EVP_PKEY_free(key);
  return publicKey;
}

TEST(ClientSigner, SignedEnvelopeCarriesTheOriginalPayloadAndVerifies) {
  const Ed25519PrivateKey privateKey = testPrivateKey();
  const Ed25519PublicKey publicKey = derivePublicKey(privateKey);
  const std::string body = "hello sequencer";

  const Bytes envelope = signPayload(payloadOf(body), privateKey);
  ASSERT_EQ(envelope.size(), kSignatureEnvelopeOverhead + body.size());

  EXPECT_TRUE(verifyEnvelopeSignature(Payload(envelope.data(), envelope.size()), publicKey));

  const Payload extracted = envelopePayload(Payload(envelope.data(), envelope.size()));
  ASSERT_EQ(extracted.size(), body.size());
  EXPECT_TRUE(std::equal(extracted.begin(), extracted.end(), payloadOf(body).begin()));
}

TEST(ClientSigner, ATamperedPayloadFailsVerificationUnderTheOriginalSignature) {
  const Ed25519PrivateKey privateKey = testPrivateKey();
  const Ed25519PublicKey publicKey = derivePublicKey(privateKey);

  Bytes envelope = signPayload(payloadOf("original"), privateKey);
  envelope.back() ^= std::byte{0xff};

  EXPECT_FALSE(verifyEnvelopeSignature(Payload(envelope.data(), envelope.size()), publicKey));
}

TEST(ClientSigner, AWrongPublicKeyFailsVerification) {
  const Ed25519PrivateKey privateKey = testPrivateKey(std::byte{0x07});
  const Ed25519PublicKey wrongPublicKey = derivePublicKey(testPrivateKey(std::byte{0x09}));

  const Bytes envelope = signPayload(payloadOf("hello"), privateKey);
  EXPECT_FALSE(verifyEnvelopeSignature(Payload(envelope.data(), envelope.size()), wrongPublicKey));
}

TEST(ClientSigner, InputShorterThanTheSignatureIsRejectedNotUndefined) {
  const Ed25519PublicKey publicKey = derivePublicKey(testPrivateKey());
  const std::string tooShort = "short";
  EXPECT_FALSE(verifyEnvelopeSignature(payloadOf(tooShort), publicKey));
  EXPECT_THROW(envelopePayload(payloadOf(tooShort)), std::invalid_argument);
}

TEST(ClientSigner, MakeEnvelopeSignatureVerifierProducesAnEquivalentCheck) {
  const Ed25519PrivateKey privateKey = testPrivateKey();
  const Ed25519PublicKey publicKey = derivePublicKey(privateKey);
  const std::function<bool(Payload)> verifier = makeEnvelopeSignatureVerifier(publicKey);

  const Bytes envelope = signPayload(payloadOf("plugged into a SignatureVerifier"), privateKey);
  EXPECT_TRUE(verifier(Payload(envelope.data(), envelope.size())));

  Bytes tampered = envelope;
  tampered.back() ^= std::byte{0xff};
  EXPECT_FALSE(verifier(Payload(tampered.data(), tampered.size())));
}

}  // namespace
}  // namespace sequencer::sdk
