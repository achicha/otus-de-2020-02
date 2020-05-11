from __future__ import print_function
import aerospike
from aerospike import exception as ex
from aerospike import predicates as p
import logging

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
logging.info('Testig local Aerospike connectivity')


class OperationalDataStore:

    def __init__(self):
        self.config = {
            'hosts': [('db', 3000)],
            'policy': {'key': aerospike.POLICY_KEY_SEND}  # specify write policies
        }
        self.key = {
            'namespace': 'test',  # similar to databases in RDBMS
            'set': 'customers',  # similar to RDBMS tables
        }
        self.client = self._connect(self.config)
        self.add_secondary_str_index('phone')

    def _connect(self, config):
        """Create a client and connect it to the cluster"""
        try:
            client = aerospike.client(config).connect()
        except:
            import sys
            logging.error("failed to connect to the cluster with", config['hosts'])
            sys.exit(1)

        return client

    def __delete__(self):
        # Close the connection to the Aerospike cluster
        self.client.close()

    def add_secondary_str_index(self, bin_name):
        try:
            self.client.index_string_create(
                ns=self.key['namespace'],
                set=self.key['set'],
                bin=bin_name,
                name='customers_phone_idx'
            )
        except ex.AerospikeError as e:
            logging.error('Index {} creation failed'.format(bin_name))
            logging.debug(e)

    def add_customer(self, customer_id, phone_number, lifetime_value):
        """Add new Customer to Database"""

        # create the key tuple identifying the record.
        # Records are addressable via a tuple of (namespace, set, key)
        key = (self.key['namespace'], self.key['set'], customer_id)

        # The record data to write to the cluster
        record = {'phone': phone_number, 'ltv': lifetime_value}

        # Put the record to the database.
        try:
            self.client.put(key, record, self.config['policy'])
        except ex.AerospikeError as e:
            logging.error('Add new customer failed')
            logging.debug(e)

    def get_ltv_by_id(self, customer_id):
        """ Get LTV value using customer_id"""

        try:
            (key, meta, record) = self.client.select((self.key['namespace'], self.key['set'], customer_id), ('ltv',))
            logging.debug(record)
        except ex.AerospikeError as e:
            logging.error('Requested non-existent customer ' + str(customer_id))
            logging.debug(e)
            record = {'ltv': None}

        return record['ltv']

    def get_ltv_by_phone(self, phone_number):
        """ Get LTV value using phone_number"""
        query = self.client.query(self.key['namespace'], self.key['set'])
        try:
            (key, meta, record) = query.where(p.equals('phone', phone_number)).select('ltv').results()[0]
            logging.debug(record)
        except ex.AerospikeError as e:
            logging.error('Requested phone number is not found ' + str(phone_number))
            logging.debug(e)
            record = {'ltv': None}

        return record['ltv']


if __name__ == "__main__":
    db = OperationalDataStore()
    # db.add_customer(customer_id=3, phone_number='123', lifetime_value=23)
    # db.get_ltv_by_id(customer_id=3)
    # db.get_ltv_by_phone(phone_number='123')

    for i in range(0, 1000):
        db.add_customer(customer_id=i, phone_number=str(i), lifetime_value=i + 1)

    for i in range(0, 1000):
        assert (i + 1 == db.get_ltv_by_id(i)), "No LTV by ID " + str(i)
        assert (i + 1 == db.get_ltv_by_phone(str(i))), "No LTV by phone " + str(i)
