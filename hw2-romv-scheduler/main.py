import os
import argparse
from user_simulator import TransactionsWorkloadSimulator
from romv_scheduler import ROMVScheduler
from serial_scheduler import SerialScheduler
from logger import Logger

TESTS_DIR = 'tests'
DEFAULT_TEST_FILENAME = 'transactions.dat'


# Helper function to parse the (optional) arguments for this script.
def args_parser():
    parser = argparse.ArgumentParser(
        description='DB implementation [236510] / Simulator for ROMV transactions scheduler.')
    parser.add_argument('--force_serial', '-s',
                        type=bool, nargs='?',
                        help='Force using a simple serial scheduler. Used mostly for debugging. ' +
                        'If not specified, use the scheduling scheme mentioned in the test file.')
    parser.add_argument('--tests', '-t',
                        type=str, nargs='*',
                        help='Test file-names to use.')
    parser.add_argument('--verbose', '-v',
                        type=bool, nargs='?',
                        help='Verbose mode. Use in order to print the variables values after each operation.')
    return parser.parse_args()


def run_scheduling_test(args, test_file_path):
    Logger().log('-----------------------------------------------------------')
    Logger().log('<<<<<<<< TEST: `{}` >>>>>>>>'.format(test_file_path))
    Logger().log('-----------------------------------------------------------')

    # The simulator is responsible for reading the workload test file and injecting the
    # transactions into the scheduler. For each transaction, the simulator simulates an
    # execution of the user program that manages this transaction. It means that values
    # that are retrieved using read-operations might be stored temporarily in local
    # variables of the program, and might be used later for as a value to write in a
    # write-operation. The simulator is also responsible for restarting a transaction
    # that ahs been aborted by the scheduler (due to a deadlock).
    simulator = TransactionsWorkloadSimulator(args.verbose)

    # Parse the workload test file and add its contents to the simulator.
    simulator.load_test_data(test_file_path)

    # Initialize the relevant scheduler.
    # By default use the scheduling scheme mentioned in the test file.
    # If a certain scheduling scheme mentioned explicitly in the arguments, use it.
    schedule_scheme = simulator.schedule
    scheduler = ROMVScheduler(schedule_scheme) if not args.force_serial else SerialScheduler()

    # Firstly, completely run the first transaction (T0), to fill the variables with some initial value.
    simulator.add_initialization_transaction_to_scheduler(scheduler)
    scheduler.run()

    # Run the transactions T1-Tn using the chosen scheduling scheme.
    # After an operation is completed for a certain transaction, the simulator would immediately spawn
    # the next operation (if there is one) to the transaction. The next time the scheduler would encounter
    # this transaction, it will contain this next operation (that has been added previously by the simulator).
    # The only exception is after a commit operation. In that case, after successfully executing this operation,
    # the scheduler would remove the completed transaction from its transactions list, so it would not encounter
    # it anymore. The scheduler runs until all transactions are completed.
    simulator.add_workload_to_scheduler(scheduler)
    scheduler.run()


if __name__ == '__main__':
    # Parse all input (optional) arguments for the scripts.
    args = args_parser()

    test_files = args.tests  # TODO: make sure it always returns a list here.
    if not test_files:
        test_files = [os.path.join(TESTS_DIR, filename)
                      for filename in os.listdir(TESTS_DIR)
                      if os.path.isfile(os.path.join(TESTS_DIR, filename))]
    for test_file_path in test_files:
        run_scheduling_test(args, test_file_path)
