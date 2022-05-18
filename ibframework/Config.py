from python_json_config import ConfigBuilder
import json

class json_config(ConfigBuilder):

    def __init__(self):

        path = "./config.js"

        # create config parser
        builder = ConfigBuilder()

        # Open config.js file
        with open(path) as jsonFile:
            self.config_data = json.load(jsonFile)

        # Parse config
        self.params = builder.parse_config(path)

        # Create containers
        self.all_contract_names = list()
        self.all_strategy_names = list()
        self.all_contracts = dict()
        self.contracts_by_type = dict()
        self.contracts_by_strat = dict()
        self.contract_names_by_strat = dict()


        # Flattens contract dictionary

        for contract in self.config_data["contracts"]:
            if self.config_data["contracts"][contract]["enabled"]:
                contract_dict = self.config_data["contracts"][contract]
                contract_dict.update(self.config_data["contracts"][contract]["override"])
                contract_dict.pop("override")
                self.all_contracts[contract]=contract_dict

    def get_all_contract_names(self):

        # Output: List containing strings of all contract names loaded from config file

        for ct in self.all_contracts.keys():
            self.all_contract_names.append(ct)
        return self.all_contract_names


    def get_all_contracts(self):

        #Output: Dictionary containing all contracts and all its properties.

        return self.all_contracts

    def get_contracts_by_type(self,type_of_contract):

        #Input: String containing secType of contract
        #Output: Dictionary containing all contracts that correspond to the input and all its properties.

        for contract in self.all_contracts:
            if self.all_contracts[contract]["sectype"] == type_of_contract:
                self.contracts_by_type[contract]=self.all_contracts[contract]
        return self.contracts_by_type

    def get_contracts_by_strat(self,strategy):

        #Input: String containing selected strategy
        #Output: Dictionary containing all contracts that correspond to the input and all its properties.
        self.get_all_strategy_names()

        for strat in self.all_strategy_names:
            self.contracts_by_strat[strat] = dict()

            for contract in self.all_contracts:
                if self.all_contracts[contract]["strategy"] == strategy:
                    self.contracts_by_strat[strat][contract]=self.all_contracts[contract]
        return self.contracts_by_strat[strategy]

    def get_contract_names_by_strat(self,strategy):

        #Input: String containing selected strategy
        #Output: List containing all contract names that correspond to the input and all its properties.

        self.get_contracts_by_strat(strategy)
        self.contract_names_by_strat[strategy]=list()

        for cn in self.contracts_by_strat[strategy].keys():
            self.contract_names_by_strat[strategy].append(cn)

        return self.contract_names_by_strat[strategy]

    def get_all_strategy_names(self):
        # Output: List containing strings of all strategy names loaded from config file
        contracts = self.get_all_contract_names()

        for cont in contracts:

            if self.all_contracts[cont]["strategy"] not in self.all_strategy_names:
                self.all_strategy_names.append(self.all_contracts[cont]["strategy"])
            else:
                pass

        return self.all_strategy_names

    def get_strategy_parameter(self, strategy, parameter):

        #Input: String containing selected strategy, string containing selected parameter
        #Output: String, integer or float containing the selected parameter.

        mode = self.params.main.mode

        if parameter not in self.config_data["strategies"][strategy][mode]:
            param = self.config_data["strategies"][strategy]["backtest"][parameter]
        else:
            param = self.config_data["strategies"][strategy][mode][parameter]
        return param

    def get_full_opt_strategy_parameters(self, strategy):

        #Input: String containing selected strategy
        #Output: Dictionary, containing full set of opt parameters.
        param = self.config_data["strategies"][strategy]["backtest"]
        #param = self.config_data["strategies"][strategy]["optimize"]
        param.update(self.config_data["strategies"][strategy]["optimize"])
        return param

    def get_contract_parameter(self, contract, parameter):

        # Input: String containing selected contract, string containing selected parameter
        # Output: String, integer or float containing the selected parameter.

        mode = self.params.main.mode
        strategy = self.all_contracts[contract]["strategy"]

        if parameter not in self.all_contracts[contract]:
            if parameter not in self.config_data["strategies"][strategy][mode]:
                param = self.config_data["strategies"][strategy]["backtest"][parameter]
            else:
                param = self.config_data["strategies"][strategy][mode][parameter]
        else:
            param = self.all_contracts[contract][parameter]
        return param

