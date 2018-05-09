
import argparse
from user_simulator import TransactionsWorkloadSimulator
from romv_scheduler import ROMVScheduler, SerialScheduler

DEFAULT_TEST_FILENAME = 'transactions.dat'


# Helper function to parse the (optional) arguments for this script.
def args_parser():
    parser = argparse.ArgumentParser(
        description='DB implementation [236510] / Simulator for ROMV transactions scheduler.')
    parser.add_argument('--sched', '-s',
                        type=str, nargs='?', choices=['RR', 'serial'],
                        help='Scheduling scheme to use. `RR` for round-robin, or `serial` for serial scheduler. ' +
                        'If not specified, use the scheduling scheme mentioned in the test file.')
    parser.add_argument('--test', '-t',
                        type=str, nargs='?', default=DEFAULT_TEST_FILENAME,
                        help='Test file-name to use.')
    return parser.parse_args()


if __name__ == '__main__':
    # Parse all input (optional) arguments for the scripts.
    args = args_parser()

    # The simulator is responsible for
    simulator = TransactionsWorkloadSimulator()

    # Parse the test file and add its contents to the simulator.
    simulator.load_test_data(workload_data_filename=args.test)

    # Initialize the relevant scheduler.
    # By default use the scheduling scheme mentioned in the test file.
    # If a certain scheduling scheme mentioned explicity in the arguments, use it.
    schedule_scheme = simulator.schedule
    if args.sched:
        schedule_scheme = args.sched
    scheduler_type = ROMVScheduler if schedule_scheme == 'RR' else SerialScheduler
    scheduler = scheduler_type()

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
