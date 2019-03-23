from sawtooth_validator.journal.batch_injector import BatchInjector
from sawtooth_validator.protobuf.transaction_pb2 import TransactionHeader
from sawtooth_validator.protobuf.transaction_pb2 import Transaction
from sawtooth_validator.protobuf.batch_pb2 import BatchHeader
from sawtooth_validator.protobuf.batch_pb2 import Batch
from sawtooth_block_info.injector import BlockInfoInjector
from sawtooth_validator.state.settings_view import SettingsView


from sawtooth_validator.protobuf.obligatory_payment_pb2 import (
    ObligatoryPaymentPayload,
    ObligatoryPaymentMethod,
)

import hashlib
from uuid import uuid4

FAMILY_NAME = 'obligatory_payment'
FAMILY_VERSIONS = ['0.1']
FAMILY_ACCOUNT = 'node_account'
NODE_STATE_ADDRESS = '0' * 69 + '2'
SETTINGS_OBLIGATORY_PAYMENT = 'remme.settings.obligatory_payment'

def hash512(data):
    return hashlib.sha512(data.encode('utf-8')
                          if isinstance(data, str) else data).hexdigest()

family_account_prefix = hash512(FAMILY_ACCOUNT)[:6]


class RemmeBatchInjector(BatchInjector):
    """Inject Remme transactions at the beginning of blocks."""

    def __init__(self, state_view_factory, signer):
        self._state_view_factory = state_view_factory
        self._signer = signer

    def create_obligatory_payment_batch(self):
        payload = ObligatoryPaymentPayload().SerializeToString()
        public_key = self._signer.get_public_key().as_hex()

        INPUTS = [
            family_account_prefix,
            NODE_STATE_ADDRESS,
            SettingsView.setting_address(SETTINGS_OBLIGATORY_PAYMENT)
        ]

        OUTPUTS = [family_account_prefix]

        header = TransactionHeader(
            signer_public_key=public_key,
            family_name=FAMILY_NAME,
            family_version=FAMILY_VERSIONS[0],
            inputs=INPUTS,
            outputs=OUTPUTS,
            dependencies=[],
            payload_sha512=hash512(payload),
            batcher_public_key=public_key,
            nonce=uuid4().hex
        ).SerializeToString()

        transaction_signature = self._signer.sign(header)

        transaction = Transaction(
            header=header,
            payload=payload,
            header_signature=transaction_signature,
        )

        header = BatchHeader(
            signer_public_key=public_key,
            transaction_ids=[transaction_signature],
            ).SerializeToString()

        batch_signature = self._signer.sign(header)

        return Batch(
            header=header,
            transactions=[transaction],
            header_signature=batch_signature,
        )

    def block_start(self, previous_block):
        """Returns an ordered list of batches to inject at the beginning of the
        block. Can also return None if no batches should be injected.
        Args:
            previous_block (Block): The previous block.
        Returns:
            A list of batches to inject.
        """
        block_info_injector = BlockInfoInjector(self._state_view_factory, self._signer)
        return block_info_injector.block_start(previous_block) + [self.create_obligatory_payment_batch()]

    def before_batch(self, previous_block, batch):
        pass

    def after_batch(self, previous_block, batch):
        pass

    def block_end(self, previous_block, batches):
        pass
