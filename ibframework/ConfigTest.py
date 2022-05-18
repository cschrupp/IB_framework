from Config import json_config


# parse config
config = json_config()



print(config.params.main.mode)

print(config.params.data.logs_output_dir)

print(config.params.version)

print(config.get_all_contract_names())

print(config.get_all_contracts())

print(config.get_contracts_by_type("STK"))

print("Get contracts by strategy", config.get_contracts_by_strat('Macd_AtrTrail_M_Dual'))
#print("Get contracts by strategy", config.get_contracts_by_strat("PercentileAtrTrail_Dual"))

print("Get contract names by strategy", config.get_contract_names_by_strat('Macd_AtrTrail_M_Dual'))
#print("Get contract names by strategy", config.get_contract_names_by_strat("PercentileAtrTrail_Dual"))

print("Get all Strategy names", config.get_all_strategy_names())

print(config.get_strategy_parameter('Macd_AtrTrail_M_Dual', "long_candle_size"))

#print("Test", config.get_contract_parameter("SPY", "highpercentile"))

#print(config.get_contract_parameter("SPY", "macd2"))

print(config.params.connection.ibapi_client_number)

print("Full opt parameters" , config.get_full_opt_strategy_parameters('Macd_AtrTrail_M_Dual'))

print("Analyzers", config.config_data["analyzers"])

#print("Strategy parameter", config.config_data["strategies"]["PercentileAtrTrail_Dual"]['candle_size'])