from flask import Flask
from flask_restx import Api, Resource, fields
import math
import pandas as pd
import numpy as np
import psycopg2 as pg

#define database class
class DataQ():

    def __init__(self, query,
                 db="postgres", server="golden-source-2020.cyozpdhauzu4.us-east-2.rds.amazonaws.com",
                 user="postgres", pwd="Bcidatabase2020"):
        self.query      = query
        self.db         = db
        self.server     = server
        self.user       = user
        self.pwd        = pwd
        self.conn       = self.db_connect()
        self.data       = pd.read_sql(self.query, self.conn)
        self.describe   = self.data.describe()
        self.head       = self.data.head(10)
        self.db_disconnect()

    def db_connect(self):
        # add try/except
        conn = None
        try:
            conn = pg.connect(
                host=self.server,
                database=self.db,
                user=self.user,
                password=self.pwd)
        except Exception as err:
            print(err)
        return conn

    def db_disconnect(self):
        self.conn.close()
        pass

#impport raw data from database
years = list(range(2018,2051))
target_level = DataQ("select * from crrem.target_levels").data
target_type = DataQ("select * from crrem.target_type").data
property_use_type = DataQ("select * from crrem.property_use_type").data
property_use_type_factor = DataQ("select * from crrem.property_use_type_factor").data
country = DataQ("select * from crrem.country").data
country_factor = DataQ("select * from crrem.country_factor").data
currency = DataQ("select * from crrem.currency").data
emission_factor = DataQ("select * from crrem.emission_factors").data
energy_conversion_factor = DataQ("select * from crrem.energy_conversion_factor").data
energy_source = DataQ("select * from crrem.energy_source").data
price = DataQ("select * from crrem.price").data
price.set_index('year',inplace=True)
price['price'] = price['price'].astype(float)
scenario_gw = DataQ("select * from crrem.scenario_gw").data
zip_nuts = DataQ("select * from crrem.zip_to_nuts").data
zip_nuts.set_index('zip_code',inplace=True)
nuts_classification = DataQ("select * from crrem.nuts_classification").data
energy_use_type = DataQ("select * from crrem.energy_use_type").data
energy_use_breakdown = DataQ("select * from crrem.energy_use_breakdown").data
hdd_cdd_by_nuts = DataQ("select * from crrem.hdd_cdd_by_nuts").data
hdd_cdd_by_nuts.set_index('nuts_code',inplace=True)

#load input epc
filename = 'domestic-E06000001-Hartlepool-certificates.csv'
epc = pd.read_csv(filename)
epc = epc.fillna('')
epc['ADDRESS'] = epc['ADDRESS1'] + epc['ADDRESS2'] + epc['ADDRESS3'] #create full adress
epc.rename(columns={"POSTCODE": "UNIT"}, inplace=True) #rename postcode column to unit
epc = epc.sort_values('INSPECTION_DATE')
epc = epc.drop_duplicates(subset=['BUILDING_REFERENCE_NUMBER'],keep='last')
epc = epc.astype({'ENERGY_CONSUMPTION_CURRENT': 'float64'}) #change data type to be acceptable to JSON 
epc = epc.set_index('BUILDING_REFERENCE_NUMBER')

#define flask app
app = Flask(__name__)
api = Api(app, version='0.1', title='CRREM API', description='MicroService for CRREM',)

ns1 = api.namespace('CRREM output', description='For both emission and energy intensity')

Output = api.model('CRREM output',
    {'building_id': fields.Integer(readOnly=True, description='The task unique identifier'),
    'VAR': fields.Float(),
    'emission stranding year': fields.Integer(),
    'emission (kg/m²)': fields.List(fields.Float()),
    'emission_target': fields.List(fields.Float()),
    'emission_excess': fields.List(fields.Float()),
    'energy (kWh/m2)': fields.List(fields.Float()),
    'energy stranding year': fields.Integer(),
    'energy_target': fields.List(fields.Float()),
    'energy_excess': fields.List(fields.Float()),
    'elec_cost': fields.List(fields.Float()),
    'gas_cost': fields.List(fields.Float()),
    'oil_cost': fields.List(fields.Float()),
    'coal_cost': fields.List(fields.Float()),
    'wood_cost': fields.List(fields.Float()),
    'excess carbon costs': fields.List(fields.Float()),
    'excess carbon savings': fields.List(fields.Float())})

@ns1.route('/<int:building_id>/<float:target_temp>/<float:RCP_scenario>')
@ns1.response(404, 'building_id not found')
@ns1.param('building_id', 'EPC building_id')
@ns1.param('target_temp', '[1.5,2.0]')
@ns1.param('RCP_scenario', '[4.5,8.5]')
class emission(Resource):
    '''Show a single todo item and lets you delete them'''
    @ns1.doc('get_todo')
    @ns1.marshal_with(Output)
    def get(self, building_id, target_temp, RCP_scenario):   

        i = building_id
        #other function parameters
        discount_factor = 0.02
        property_price = 500000
        
        if target_temp == 1.5:
            gw_scenario_id = 1
        elif target_temp == 2.0:
            gw_scenario_id = 2

         # 1.Data preparation GHG emission target
        #find property type id
        if epc['PROPERTY_TYPE'][i] == 'Bungalow' or 'House'or 'Park home':
            property_type_code = 'RES' #single-family
        else:
            property_type_code = 'RESM' #multi-family
        property_type_id = property_use_type[property_use_type['use_type_code']==property_type_code]['use_type_id'].iloc[0]

        #specify target based on property type/target type/scenario
        years = list(range(2018,2051))
        emission_target = target_level[(target_level['prop_use_type_id']==property_type_id) & (target_level['target_type_id']==1) & (target_level['gw_scenario_id']==gw_scenario_id)]['target_level']
        emission_target.index = years
        energy_target = target_level[(target_level['prop_use_type_id']==property_type_id) & (target_level['target_type_id']==2) & (target_level['gw_scenario_id']==gw_scenario_id)]['target_level']
        energy_target.index = years

        #HDD/CDD projection
        #HDD - HDD index
        RCP = 'RCP'+str(RCP_scenario)

        if epc['UNIT'][i] != 0:
            NUTS3 = 'UK' + epc['UNIT'][i].split(' ')[0]

        years_index = list(range(3,36))
        HDD = pd.DataFrame(columns = years_index, index=[1])
        for year in years_index:
            if RCP == 'RCP4.5':
                if len(zip_nuts.loc[NUTS3]) > 1:
                    HDD.iloc[0,year-3] = (hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['hdd_2015'] + year*hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['hdd_rcp45_pa'])/(hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['hdd_2015'] + 3*hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['hdd_rcp45_pa'])
                else:
                    HDD.iloc[0,year-3] = (hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3][0]]['hdd_2015'] + year*hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3][0]]['hdd_rcp45_pa'])/(hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3][0]]['hdd_2015'] + 3*hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3][0]]['hdd_rcp45_pa'])
            elif RCP == 'RCP8.5':
                if len(zip_nuts.loc[NUTS3]) > 1:
                    HDD.iloc[0,year-3] = (hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['hdd_2015'] + year*hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['hdd_rcp85_pa'])/(hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['hdd_2015'] + 3*hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['hdd_rcp85_pa'])
                else:
                    HDD.iloc[0,year-3] = (hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3][0]]['hdd_2015'] + year*hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3][0]]['hdd_rcp85_pa'])/(hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3][0]]['hdd_2015'] + 3*hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3][0]]['hdd_rcp85_pa'])

        #assumption1: if one zip macthes multiple nuts, take the first nuts
        #CDD - CDD index                
        CDD = pd.DataFrame(columns = years_index, index=[1])
        for year in years_index:
            if RCP == 'RCP4.5':
                if len(zip_nuts.loc[NUTS3]) > 1:
                    CDD.iloc[0,year-3] = (hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['cdd_2015'] + year*hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['cdd_rcp45_pa'])/(hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['cdd_2015'] + 3*hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['cdd_rcp45_pa'])
                else:
                    CDD.iloc[0,year-3] = (hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3][0]]['cdd_2015'] + year*hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3][0]]['cdd_rcp45_pa'])/(hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3][0]]['cdd_2015'] + 3*hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3][0]]['cdd_rcp45_pa'])
            else:
                if len(zip_nuts.loc[NUTS3]) > 1:
                    CDD.iloc[0,year-3] = (hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['cdd_2015'] + year*hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['cdd_rcp85_pa'])/(hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['cdd_2015'] + 3*hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['cdd_rcp85_pa'])
                else:
                    CDD.iloc[0,year-3] = (hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3][0]]['cdd_2015'] + year*hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3][0]]['cdd_rcp85_pa'])/(hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3][0]]['cdd_2015'] + 3*hdd_cdd_by_nuts.loc[zip_nuts.loc[NUTS3][0]]['cdd_rcp85_pa'])
        CDD.columns = list(range(2018,2051))
        HDD.columns = list(range(2018,2051))
        HDD.fillna(0,inplace = True)
        CDD.fillna(0,inplace = True)  

        #2. GHG emission projection
        #emission data
        current_emission = epc['CO2_EMISS_CURR_PER_FLOOR_AREA'][i]
        elec_heat = energy_use_breakdown['percentage'][0]/100 #share of electricity for heating in UK
        elec_cool = energy_use_breakdown['percentage'][1]/100
        fuel_heat = energy_use_breakdown['percentage'][2]/100
        grid_uk = emission_factor['value'] #emission factor for UK

        #electricity usage share 
        if 'electricity' in epc['MAIN_FUEL'][i]:
            electricity_share = 0.7
        else:
            electricity_share = 0.3

        emission = pd.Series(0,index=list(range(2018,2051)))

        #assumption 2: district heating/cooling and fugitive emission not considered
        for year in years:
            if HDD.iloc[0,year-2018] != 0:
                emission.iloc[year-2018] = current_emission*(electricity_share*grid_uk[year-2018]/grid_uk[0]*(1+elec_heat*(HDD.iloc[0,year-2018]-1)+elec_cool*(CDD.iloc[0,year-2018]-1))+ ((1-electricity_share)*HDD.iloc[0,year-2018]/HDD.iloc[0,2018-2018]*(1+fuel_heat*(HDD.iloc[0,year-2018]-1))))
            if HDD.iloc[0,year-2018] == 0:
                emission.iloc[year-2018] = current_emission*(electricity_share*grid_uk[year-2018]/grid_uk[0]*(1+elec_heat*(HDD.iloc[0,year-2018]-1)+elec_cool*(CDD.iloc[0,year-2018]-1)))

        emission_baseline = current_emission*len(emission.T) #create baseline pandas series with same index as emission
        emission_excess = emission - emission_target
        if len(emission_excess[emission_excess > 0]) == 0:
            emission_stranding_year = 2050
        else:
            emission_stranding_year = emission_excess[emission_excess > 0].index[0]
            
        #3. energy projection 
        #energy energy projection
        current_energy = epc['ENERGY_CONSUMPTION_CURRENT'][i]
        energy = pd.Series(0,index=list(range(2018,2051)))
        for year in years:
            if HDD.iloc[0,year-2018] != 0:
                energy.iloc[year-2018] = current_energy*(electricity_share*grid_uk[year-2018]/grid_uk[0]*(1+elec_heat*(HDD.iloc[0,year-2018]-1)+elec_cool*(CDD.iloc[0,year-2018]-1))+ ((1-electricity_share)*HDD.iloc[0,year-2018]/HDD.iloc[0,2018-2018]*(1+fuel_heat*(HDD.iloc[0,year-2018]-1))))
            if HDD.iloc[0,year-2018] == 0:
                energy.iloc[year-2018] = current_energy*(electricity_share*grid_uk[year-2018]/grid_uk[0]*(1+elec_heat*(HDD.iloc[0,year-2018]-1)+elec_cool*(CDD.iloc[0,year-2018]-1)))
                
        energy_baseline = current_energy*len(energy.T) #create baseline pandas series with same index as energy
        energy_excess = energy - energy_target 
        if len(energy_excess[energy_excess > 0]) == 0:
            energy_stranding_year = 2050
        else:
            energy_stranding_year = energy_excess[energy_excess > 0].index[0]
            
        #4. energy costs
        elec_cost = price[price['source']=='elect_incl_vat']['price'] #electricity price incl. VAT
        gas_cost = price[price['source']=='gas_incl_vat']['price'] #gas price incl. VAT
        oil_cost = price[price['source']=='oil_incl_vat']['price'] #oil price incl. VAT
        wood_cost = price[price['source']=='wood_incl_vat']['price'] #wood pellets price incl. VAT
        coal_cost = price[price['source']=='coal_incl_vat']['price'] #coal price incl. VAT
        carbon_price = price[price['source']=='carbon']['price'] #carbon price incl. VAT

        total_energy = current_energy

        if 'electricity' in epc['MAIN_FUEL'][i]:
            elec_cost = total_energy*elec_cost*0.7
            gas_cost = total_energy*gas_cost*0.3
            oil_cost = total_energy*oil_cost*0
            coal_cost = total_energy*coal_cost*0
            wood_cost = total_energy*wood_cost*0
        elif 'gas' in epc['MAIN_FUEL'][i]:
            elec_cost = total_energy*elec_cost*0.3
            gas_cost = total_energy*gas_cost*0.7
            oil_cost = total_energy*oil_cost*0
            coal_cost = total_energy*coal_cost*0
            wood_cost = total_energy*wood_cost*0
        elif 'oil' in epc['MAIN_FUEL'][i]:
            elec_cost = total_energy*elec_cost*0.5
            gas_cost = total_energy*gas_cost*0.5
            oil_cost = total_energy*oil_cost*0.7
            coal_cost = total_energy*coal_cost*0
            wood_cost = total_energy*wood_cost*0
        elif 'coal' in epc['MAIN_FUEL'][i]:
            elec_cost = total_energy*elec_cost*0.15
            gas_cost = total_energy*gas_cost*0.15 
            oil_cost = total_energy*oil_cost*0
            coal_cost = total_energy*coal_cost*0.7
            wood_cost = total_energy*wood_cost*0
        elif 'wood' in epc['MAIN_FUEL'][i]:
            elec_cost = total_energy*elec_cost*0.15
            gas_cost = total_energy*gas_cost*0.15
            oil_cost = total_energy*oil_cost*0
            coal_cost = total_energy*coal_cost*0
            wood_cost = total_energy*wood_cost*0.7
        else:
            elec_cost = total_energy*elec_cost*0.5
            gas_cost = total_energy*gas_cost*0.5
            oil_cost = total_energy*oil_cost*0
            coal_cost = total_energy*coal_cost*0
            wood_cost = total_energy*wood_cost*0

        #5. excess carbon costs and value at risk
        floor_area = epc['TOTAL_FLOOR_AREA'][i]
        total_emission = emission * floor_area
        total_target = emission_target * floor_area
        excess_cost = carbon_price * (total_emission-total_target)

        years = list(range(2018,2051))
        costs = pd.Series(np.nan, index = years)
        value = pd.Series(np.nan, index = years)
        for year in years:
            if excess_cost[year]<0:
                costs[year] = 0
                value[year] = excess_cost[year]
            else:
                costs[year] = excess_cost[year]
                value[year] = 0
                
        discount_costs = costs.tolist().copy()
        discount_value = value.tolist().copy()

        for year in list(range(2018,2051)):
            discount_costs[year-2018] = discount_costs[year-2018]/(1+discount_factor)**(year-2018)
            discount_value[year-2018] = discount_value[year-2018]/(1+discount_factor)**(year-2018)

        VAR = (sum(discount_costs) + sum(discount_value))/property_price

        return {
        'building_id': building_id,
        'VAR': VAR,
        'emission stranding year': emission_stranding_year,
        'emission (kg/m²)': emission.tolist(),
        'emission_target': emission_target.tolist(),
        'emission_excess': emission_excess.tolist(),
        'energy (kWh/m2)': energy.tolist(),
        'energy stranding year': energy_stranding_year,
        'energy_target': energy_target.tolist(),
        'energy_excess': energy_excess.tolist(),
        'elec_cost': elec_cost.tolist(),
        'gas_cost': gas_cost.tolist(),
        'oil_cost': oil_cost.tolist(),
        'coal_cost': coal_cost.tolist(),
        'wood_cost': wood_cost.tolist(),
        'excess carbon costs': costs.tolist(),
        'excess carbon savings': value.tolist(),}

    
if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port='5001')