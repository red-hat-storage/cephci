# -*- code: utf-8 -*-
"""SSL Cert generation module."""

import ipaddress
import logging
from datetime import datetime, timedelta

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import NoEncryption
from cryptography.x509 import (
    AuthorityKeyIdentifier,
    BasicConstraints,
    CertificateBuilder,
    IPAddress,
    Name,
    NameAttribute,
    NameOID,
    SubjectAlternativeName,
    SubjectKeyIdentifier,
    random_serial_number,
)

LOG = logging.getLogger(__name__)


class CertificateGenerator:
    def __init__(self, nodes, common_name, ips=None, key_size=4096, days_valid=3650):
        self.nodes = nodes
        self.common_name = common_name
        self.ips = ips
        self.key_size = key_size
        self.days_valid = days_valid
        self.private_key = None
        self.certificate = None

    def generate_key(self):
        """Generate an RSA private key."""
        self.private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=self.key_size, backend=default_backend()
        )

    def generate_certificate(self, is_server=False):
        """Generate a self-signed certificate with optional SAN."""
        if self.private_key is None:
            raise ValueError("Private key is not generated.")

        subject = issuer = Name([NameAttribute(NameOID.COMMON_NAME, self.common_name)])

        # Create the CertificateBuilder
        builder = CertificateBuilder(
            subject_name=subject,
            issuer_name=issuer,
            public_key=self.private_key.public_key(),
            serial_number=random_serial_number(),
            not_valid_before=datetime.utcnow(),
            not_valid_after=datetime.utcnow() + timedelta(days=self.days_valid),
        )

        if is_server and self.ips:
            san = SubjectAlternativeName(
                [IPAddress(ipaddress.ip_address(ip)) for ip in self.ips]
            )
            builder = builder.add_extension(san, critical=False)

        builder = builder.add_extension(
            BasicConstraints(ca=True, path_length=None), critical=True
        )

        # Generate Subject Key Identifier
        subject_key_identifier = SubjectKeyIdentifier.from_public_key(
            self.private_key.public_key()
        )
        builder = builder.add_extension(subject_key_identifier, critical=False)

        # Add Authority Key Identifier
        authority_key_identifier = AuthorityKeyIdentifier.from_issuer_public_key(
            self.private_key.public_key()
        )
        builder = builder.add_extension(authority_key_identifier, critical=False)

        # Create the certificate
        self.certificate = builder.sign(
            self.private_key, hashes.SHA256(), default_backend()
        )

    def save_files(self, key_path, cert_path, dest_path="~"):
        """Save the private key and certificate to files.

        Args:
            key_file_path: Private Key file path
            cert_file_path: Private Cert file path
            dest_path: Destination path to save these files
        """
        if self.private_key is None or self.certificate is None:
            raise ValueError("Key or certificate is not generated.")

        private_key_pem = self.private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=NoEncryption(),
        )

        certificate_pem = self.certificate.public_bytes(
            encoding=serialization.Encoding.PEM
        )

        with open(key_path, "wb") as key_file:
            key_file.write(private_key_pem)

        with open(cert_path, "wb") as cert_file:
            cert_file.write(certificate_pem)

        for node in self.nodes:
            node.upload_file(key_path, f"{dest_path}/{key_path}", sudo=True)
            node.upload_file(cert_path, f"{dest_path}/{cert_path}", sudo=True)

            LOG.info(
                f"""Private key and certificate have been generated and
                saved to {dest_path}/{key_path} and {dest_path}/{cert_path}."""
            )

        return private_key_pem.decode("ascii"), certificate_pem.decode("ascii")
