

# Collection of patterns used for parsing the input workload test file.
# Used by the method: TransactionSimulator.parse_transaction_from_test_line(..)
# Used by the method: TransactionSimulator.parse_and_add_operation(..)
class TransactionParsingPatterns:
    number_pattern = '(([1-9][0-9]*)|0)'
    identifier_pattern = '[a-zA-Z_][a-zA-Z_0-9]*'
    const_value_pattern = number_pattern
    local_var_identifier_pattern = '(?P<local_var_identifier>' + identifier_pattern + ')'
    var_identifier_pattern = '(?P<var_identifier>' + identifier_pattern + ')'
    optional_line_comment_pattern = '(?P<line_comment>((\/\/)|(\#)).*)?'

    comment_line_pattern = '^[\s]*' + optional_line_comment_pattern + '$'

    scheduling_scheme_num = '(?P<scheduling_scheme_num>[12])'
    num_of_transactions = '(?P<num_of_transactions>' + number_pattern + ')'
    test_first_line_pattern = '^' + scheduling_scheme_num + \
                              '[\s]+' + num_of_transactions + \
                              '[\s]*' + optional_line_comment_pattern + '$'

    transaction_type_pattern = '(?P<transaction_type>[UR])'
    transaction_id_pattern = '(?P<transaction_id>' + number_pattern + ')'
    transaction_header_pattern = transaction_type_pattern + '[\s]+' + \
                                 transaction_id_pattern + '[\s]+' + \
                                 '(?P<transaction_operations>.*)\;'
    transaction_line_pattern = '^' + transaction_header_pattern + \
                               '[\s]*' + optional_line_comment_pattern + '$'

    write_value_pattern = '(?P<write_value>' + local_var_identifier_pattern + '|' + const_value_pattern + ')'
    write_operation_pattern = '(w\(' + var_identifier_pattern + '[\s]*\,[\s]*' + write_value_pattern + '\))'
    read_operation_pattern = '(' + local_var_identifier_pattern + \
                             '[\s]*\=[\s]*' + \
                             'r\(' + var_identifier_pattern + '\))'
    commit_operation_pattern = '(c' + transaction_id_pattern + ')'
    suspend_operation_pattern = '(suspend)'
    operation_pattern = '(?P<operation>(' + read_operation_pattern + '|' + write_operation_pattern + \
                        '|' + commit_operation_pattern + '|' + suspend_operation_pattern + '))'
    operations_pattern = '(' + operation_pattern + '[\s]+)*'
