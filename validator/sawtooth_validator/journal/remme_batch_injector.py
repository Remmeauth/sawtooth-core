import hashlib
from uuid import uuid4

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
from sawtooth_validator.protobuf.node_account_pb2 import (
    NodeAccountInternalTransferPayload,
    NodeAccountMethod,
)
from sawtooth_validator.protobuf.consensus_account_pb2 import (
    ConsensusAccountMethod,
    ConsensusAccount,
)
from sawtooth_validator.protobuf.transaction_pb2 import (
    TransactionPayload,
    EmptyPayload,
)


def hash512(data):
    return hashlib.sha512(
        data.encode("utf-8") if isinstance(data, str) else data
    ).hexdigest()


NODE_STATE_ADDRESS = "0" * 69 + "2"
ZERO_ADRESS = "0" * 70
CONSENSUS_ADDRESS = hash512("consensus_account")[:6] + "0" * 64

SETTINGS_OBLIGATORY_PAYMENT = "remme.settings.obligatory_payment"
SETTINGS_MINIMUM_STAKE = 'remme.settings.minimum_stake'
SETTINGS_COMMITTEE_SIZE = 'remme.settings.committee_size'
SETTINGS_BLOCKCHAIN_TAX = 'remme.settings.blockchain_tax'
SETTINGS_MIN_SHARE = 'remme.settings.min_share'
SETTINGS_GENESIS_OWNERS = 'remme.settings.genesis_owners'

NAMESPACE = '00b10c'
CONFIG_ADDRESS = NAMESPACE + '01' + '0' * 62
BLOCK_INFO_NAMESPACE = NAMESPACE + '00'

family_account_prefix = hash512("node_account")[:6]


class RemmeBatchInjector(BatchInjector):
    """Inject Remme transactions at the beginning of blocks."""

    def __init__(self, state_view_factory, signer):
        self._state_view_factory = state_view_factory
        self._signer = signer

    @property
    def public_key(self):
        return self._signer.get_public_key().as_hex()

    def create_obligatory_payment_batch(self):
        inputs = [
            family_account_prefix,
            NODE_STATE_ADDRESS,
            SettingsView.setting_address(SETTINGS_OBLIGATORY_PAYMENT),
        ]
        outputs = [family_account_prefix]
        method = ObligatoryPaymentMethod.PAY_OBLIGATORY_PAYMENT
        payload = ObligatoryPaymentPayload()

        return self._create_batch(inputs, outputs, method, payload, "obligatory_payment", "0.1")

    def create_do_bet_batch(self):
        inputs = [
            family_account_prefix,
            NODE_STATE_ADDRESS,
            CONSENSUS_ADDRESS,
            ZERO_ADRESS,
            SettingsView.setting_address(SETTINGS_GENESIS_OWNERS)
        ]
        outputs = inputs
        method = NodeAccountMethod.DO_BET
        payload = NodeAccountInternalTransferPayload()

        return self._create_batch(inputs, outputs, method, payload, "bet", "0.1")

    def create_pay_reward_batch(self):
        inputs = [
            family_account_prefix,
            hash512("account")[:6],

            SettingsView.setting_address(SETTINGS_MINIMUM_STAKE),
            SettingsView.setting_address(SETTINGS_COMMITTEE_SIZE),
            SettingsView.setting_address(SETTINGS_BLOCKCHAIN_TAX),
            SettingsView.setting_address(SETTINGS_MIN_SHARE),

            CONFIG_ADDRESS,
            BLOCK_INFO_NAMESPACE,

            CONSENSUS_ADDRESS,
            ZERO_ADRESS,
        ]
        outputs = [
            family_account_prefix,
            hash512("account")[:6],
            CONSENSUS_ADDRESS,
            ZERO_ADRESS,
        ]
        method = ConsensusAccountMethod.SEND_REWARD
        payload = EmptyPayload()

        return self._create_batch(inputs, outputs, method, payload, "consensus_account", "0.1")

    def get_block_start_batch_list_methods(self):
        """Methods which required to be executed in batch injector at block start
        """
        yield from [
            self.create_pay_reward_batch,
            self.create_obligatory_payment_batch,
            self.create_do_bet_batch,
        ]


    def block_start(self, previous_block):
        """Returns an ordered list of batches to inject at the beginning of the
        block. Can also return None if no batches should be injected.
        Args:
            previous_block (Block): The previous block.
        Returns:
            A list of batches to inject.
        """
        block_info_injector = BlockInfoInjector(self._state_view_factory, self._signer)
        return block_info_injector.block_start(previous_block) + [
            batch_method()
            for batch_method in self.get_block_start_batch_list_methods()
        ]

    def before_batch(self, previous_block, batch):
        pass

    def after_batch(self, previous_block, batch):
        pass

    def block_end(self, previous_block, batches):
        pass

    def _create_batch(self, inputs, outputs, method, payload, family_name, family_version):
        transaction_payload = TransactionPayload()
        transaction_payload.method = method
        transaction_payload.data = payload.SerializeToString()

        serialized_transaction_payload = transaction_payload.SerializeToString()

        header = TransactionHeader(
            signer_public_key=self.public_key,
            family_name=family_name,
            family_version=family_version,
            inputs=inputs,
            outputs=outputs,
            dependencies=[],
            payload_sha512=hash512(serialized_transaction_payload),
            batcher_public_key=self.public_key,
            nonce=uuid4().hex,
        ).SerializeToString()

        transaction_signature = self._signer.sign(header)

        transaction = Transaction(
            header=header,
            payload=serialized_transaction_payload,
            header_signature=transaction_signature,
        )

        header = BatchHeader(
            signer_public_key=self.public_key, transaction_ids=[transaction_signature]
        ).SerializeToString()

        batch_signature = self._signer.sign(header)

        return Batch(
            header=header, transactions=[transaction], header_signature=batch_signature
        )
