from flask import Flask
from flask_restx import Api, Resource, fields
import math
import pandas as pd
import numpy as np
import xlwings as xw
from pandas.api.types import is_numeric_dtype

#load worksheets
Input = xw.Book('input.xlsx').sheets['Input']
settings = xw.Book('input.xlsx').sheets['Settings']
asset = xw.Book('input.xlsx').sheets['Asset']
data = xw.Book('input.xlsx').sheets['Back-end']
zip_nuts = pd.read_excel(pd.ExcelFile('input.xlsx'), 'Back-end2')
zip_nuts.set_index('ZIP Code to NUTS mapping', inplace = True)
nuts_id = pd.read_excel(pd.ExcelFile('input.xlsx'), 'Back-end4')
nuts_id.set_index('NUTS_ID', inplace = True)
energy_price = xw.Book('input.xlsx').sheets['Energy Price']

#import raw data from input.xlsx 
years = list(range(2018,2051))
Target = pd.read_excel(pd.ExcelFile('input.xlsx'), 'GHG Target') #GHG decarbonisation target (kgco2/m2)
Target.set_index('ta', inplace = True)
Target_energy = pd.read_excel(pd.ExcelFile('input.xlsx'), 'Energy Target') #Energy consumption target (kwh/m2)
Target_energy.set_index('ta', inplace = True)
emission_factor = pd.DataFrame(data.range('A32:AK60').value) #Emissions factor: electricity & heat (trade adjusted) - A23
emission_factor.columns = emission_factor.iloc[0]
emission_factor = emission_factor[1:]
emission_factor.set_index('Country', inplace = True)
emission_factor.columns = list(range(2015,2051))
X2 = pd.DataFrame(data.range('X2:Y12').value) #BPN: property factor
X2.set_index(0, inplace = True)
X2.fillna(0, inplace = True)
Z4 = data.range('Z4').value
Z6 = data.range('Z6').value
AB3 = data.range('AB3').value #Margianl abatement cost
AB4 = data.range('AB4').value #Margianl abatement cost
AF2 = pd.DataFrame(data.range('AF2:AG29').value)
AF2.set_index(0, inplace = True)

#load input epc
filename = 'domestic-E06000001-Hartlepool-certificates.csv'
epc = pd.read_csv(filename)
epc = epc.fillna('')
epc['ADDRESS'] = epc['ADDRESS1'] + epc['ADDRESS2'] + epc['ADDRESS3'] #create full adress
epc.rename(columns={"POSTCODE": "UNIT"}, inplace=True) #rename postcode column to unit
epc = epc.drop_duplicates() #drop duplicated records, over 3 million rows
#epc = epc[epc.BUILDING_REFERENCE_NUMBER!='<?>'] #drop wrong id
epc = epc.astype({'ENERGY_CONSUMPTION_CURRENT': 'float64'}) #change data type to be acceptable to JSON 
epc = epc.set_index('BUILDING_REFERENCE_NUMBER')

#define flask app
app = Flask(__name__)
api = Api(app, version='0.1', title='CRREM API', description='MicroService for CRREM',)

ns1 = api.namespace('CRREM output', description='For both emission and energy intensity')

Output = api.model('CRREM output',
    {'building_id': fields.Integer(readOnly=True, description='The task unique identifier'),
    'VAR1': fields.Float(),
    'VAR2': fields.Float(),
    'emission (kg/m²)': fields.List(fields.Float()),
    'emission stranding year1': fields.Integer(),
    'emission_target1': fields.List(fields.Float()),
    'emission_excess1': fields.List(fields.Float()),
    'emission stranding year2': fields.Integer(),
    'emission_target2': fields.List(fields.Float()),
    'emission_excess2': fields.List(fields.Float()),
    'emission_baseline': fields.List(fields.Float()),
    'energy (kWh/m2)': fields.List(fields.Float()),
    'energy stranding year1': fields.Integer(),
    'energy_target1': fields.List(fields.Float()),
    'energy_excess1': fields.List(fields.Float()),
    'energy stranding year2': fields.Integer(),
    'energy_target2': fields.List(fields.Float()),
    'energy_excess2': fields.List(fields.Float()),
    'energy_baseline': fields.List(fields.Float()),
    'elec_cost': fields.List(fields.Float()),
    'gas_cost': fields.List(fields.Float()),
    'oil_cost': fields.List(fields.Float()),
    'coal_cost': fields.List(fields.Float()),
    'wood_cost': fields.List(fields.Float()),
    'excess carbon costs1': fields.List(fields.Float()),
    'excess carbon savings1': fields.List(fields.Float()),
    'excess carbon costs2': fields.List(fields.Float()),
    'excess carbon savings2': fields.List(fields.Float()),
    'retrofit costs1': fields.List(fields.Float()),
    'retrofit costs2': fields.List(fields.Float()),
    'xaxis': fields.List(fields.Float())})

@ns1.route('/<int:building_id>')
@ns1.response(404, 'building_id not found')
@ns1.param('building_id', 'EPC building_id')
class emission(Resource):
    '''Show a single todo item and lets you delete them'''
    @ns1.doc('get_todo')
    @ns1.marshal_with(Output)
    def get(self, building_id):   
        #1. Stranding diagram    
        #i = epc[epc['BUILDING_REFERENCE_NUMBER']==building_id].index.tolist()[0] 
        i = building_id
        #data = epc[epc['BUILDING_REFERENCE_NUMBER']==building_id]
        data = epc
        data['PRICE'] = 5000000 #input dummy data for price
        data['discount factor'] = 0.1 #input dummy discount factor

        if 'electricity' in data['MAIN_FUEL'][i]:
            electricity_share = 0.7
        else:
            electricity_share = 0.3

        if data['PROPERTY_TYPE'][i] == 'Bungalow' or 'Flat' or 'House' or 'Mansionette' or 'Park home':
            property_type_code = 'RES'
        else:
            property_type_code = data['PROPERTY_TYPE'][i] #need further investigation with non-domestic data

        #emission data
        current_emission = data['CO2_EMISS_CURR_PER_FLOOR_AREA'][i]
        elec_heat = 0.103 #share of electricity for heating in UK
        elec_cool = 0.129
        fuel_heat = 0.779
        grid_uk = emission_factor.loc['UK'][3:] #emission factor for UK

        #HDD - HDD index
        RCP = 'RCP4.5'

        if data['UNIT'][i] != 0:
            NUTS3 = 'UK' + data['UNIT'][i].split(' ')[0]

        years_index = list(range(3,36))
        HDD = pd.DataFrame(columns = years_index, index=[1])
        for year in years_index:
            if RCP == 'RCP4.5':
                if len(zip_nuts.loc[NUTS3]) > 1:
                    HDD.iloc[0,year-3] = (nuts_id.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['HDD_2015'] + year*nuts_id.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['HDD_45_pa'])/(nuts_id.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['HDD_2015'] + 3*nuts_id.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['HDD_45_pa'])
                else:
                    HDD.iloc[0,year-3] = (nuts_id.loc[zip_nuts.loc[NUTS3][0]]['HDD_2015'] + year*nuts_id.loc[zip_nuts.loc[NUTS3][0]]['HDD_45_pa'])/(nuts_id.loc[zip_nuts.loc[NUTS3][0]]['HDD_2015'] + 3*nuts_id.loc[zip_nuts.loc[NUTS3][0]]['HDD_45_pa'])
            else:
                if len(zip_nuts.loc[NUTS3]) > 1:
                    HDD.iloc[0,year-3] = (nuts_id.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['HDD_2015'] + year*nuts_id.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['HDD_85_pa'])/(nuts_id.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['HDD_2015'] + 3*nuts_id.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['HDD_85_pa'])
                else:
                    HDD.iloc[0,year-3] = (nuts_id.loc[zip_nuts.loc[NUTS3][0]]['HDD_2015'] + year*nuts_id.loc[zip_nuts.loc[NUTS3][0]]['HDD_85_pa'])/(nuts_id.loc[zip_nuts.loc[NUTS3][0]]['HDD_2015'] + 3*nuts_id.loc[zip_nuts.loc[NUTS3][0]]['HDD_85_pa'])

        #assumption 1: zip_nuts mapping: if one zip macthes multiple nuts, take the first nuts
        #CDD - CDD index                
        CDD = pd.DataFrame(columns = years_index, index=[1])
        for year in years_index:
            if RCP == 'RCP4.5':
                if len(zip_nuts.loc[NUTS3]) > 1:
                    CDD.iloc[0,year-3] = (nuts_id.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['CDD_2015'] + year*nuts_id.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['CDD_45_pa'])/(nuts_id.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['CDD_2015'] + 3*nuts_id.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['CDD_45_pa'])
                else:
                    CDD.iloc[0,year-3] = (nuts_id.loc[zip_nuts.loc[NUTS3][0]]['CDD_2015'] + year*nuts_id.loc[zip_nuts.loc[NUTS3][0]]['CDD_45_pa'])/(nuts_id.loc[zip_nuts.loc[NUTS3][0]]['CDD_2015'] + 3*nuts_id.loc[zip_nuts.loc[NUTS3][0]]['CDD_45_pa'])
            else:
                if len(zip_nuts.loc[NUTS3]) > 1:
                    CDD.iloc[0,year-3] = (nuts_id.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['CDD_2015'] + year*nuts_id.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['CDD_85_pa'])/(nuts_id.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['CDD_2015'] + 3*nuts_id.loc[zip_nuts.loc[NUTS3].iloc[0][0]]['CDD_85_pa'])
                else:
                    CDD.iloc[0,year-3] = (nuts_id.loc[zip_nuts.loc[NUTS3][0]]['CDD_2015'] + year*nuts_id.loc[zip_nuts.loc[NUTS3][0]]['CDD_85_pa'])/(nuts_id.loc[zip_nuts.loc[NUTS3][0]]['CDD_2015'] + 3*nuts_id.loc[zip_nuts.loc[NUTS3][0]]['CDD_85_pa'])
        CDD.columns = list(range(2018,2051))
        HDD.columns = list(range(2018,2051))
        HDD.fillna(0,inplace = True)
        CDD.fillna(0,inplace = True)  

        #assumption: 2. constant relative differences between each subsector
        emission_target_uk = pd.DataFrame()
        for index in range(len(Target)):
            if 'UK' in Target.index[index]:
                emission_target_uk = pd.concat([emission_target_uk,pd.DataFrame(Target.loc[Target.index[index]]).T])
        emission_target_uk.loc['UK_RES_1.5'] = emission_target_uk.loc['UK_OFF_1.5']*136/154
        emission_target_uk.loc['UK_RES_2'] = emission_target_uk.loc['UK_OFF_2']*136/154

        years = list(range(2018,2051))
        emission = pd.Series(index=list(range(2018,2051)))

        #assumption 3: district heating/cooling and fugitive emission not considered
        for year in years:
            if HDD.iloc[0,year-2018] != 0:
                emission.iloc[year-2018] = current_emission*(electricity_share*grid_uk[year]/grid_uk[2018]*(1+elec_heat*(HDD.iloc[0,year-2018]-1)+elec_cool*(CDD.iloc[0,year-2018]-1))+ ((1-electricity_share)*HDD.iloc[0,year-2018]/HDD.iloc[0,2018-2018]*(1+fuel_heat*(HDD.iloc[0,year-2018]-1))))
            if HDD.iloc[0,year-2018] == 0:
                emission.iloc[year-2018] = current_emission*(electricity_share*grid_uk[year]/grid_uk[2018]*(1+elec_heat*(HDD.iloc[0,year-2018]-1)+elec_cool*(CDD.iloc[0,year-2018]-1)))

        asset_code1 = 'UK' + '_' + property_type_code + '_' + '1.5'
        asset_code2 = 'UK' + '_' + property_type_code + '_' + '2'

        emission_baseline = [emission[2018]]*len(emission.T) #create baseline pandas series with same index as emission
        emission_target1 = emission_target_uk.loc[asset_code1]
        emission_excess1 = emission - emission_target1 
        emission_stranding_year1 = emission_excess1[emission_excess1 > 0].index[0]
        emission_target2 = emission_target_uk.loc[asset_code2]
        emission_excess2 = emission - emission_target2
        emission_stranding_year2 = emission_excess2[emission_excess2 > 0].index[0]

        #2. energy reduction pathway
        energy_target_uk = pd.DataFrame()
        for index in range(len(Target_energy)):
            if 'UK' in Target.index[index]:
                energy_target_uk = pd.concat([energy_target_uk,pd.DataFrame(Target_energy.loc[Target_energy.index[index]]).T])

        energy_target_uk.loc['UK_RES_1.5'] = energy_target_uk.loc['UK_OFF_1.5']*136/154
        energy_target_uk.loc['UK_RES_2'] = energy_target_uk.loc['UK_OFF_2']*136/154

        #energy energy projection
        current_energy = data['ENERGY_CONSUMPTION_CURRENT'][i]
        years = list(range(2018,2051))
        energy = pd.Series(index=list(range(2018,2051)))
        for year in years:
            if HDD.iloc[0,year-2018] != 0:
                energy.iloc[year-2018] = current_energy*(electricity_share*grid_uk[year]/grid_uk[2018]*(1+elec_heat*(HDD.iloc[0,year-2018]-1)+elec_cool*(CDD.iloc[0,year-2018]-1))+ ((1-electricity_share)*HDD.iloc[0,year-2018]/HDD.iloc[0,2018-2018]*(1+fuel_heat*(HDD.iloc[0,year-2018]-1))))
            if HDD.iloc[0,year-2018] == 0:
                energy.iloc[year-2018] = current_energy*(electricity_share*grid_uk[year]/grid_uk[2018]*(1+elec_heat*(HDD.iloc[0,year-2018]-1)+elec_cool*(CDD.iloc[0,year-2018]-1)))

        energy_baseline = [energy[2018]]*len(energy.T) #create baseline pandas series with same index as energy
        energy_target1 = energy_target_uk.loc[asset_code1]
        energy_excess1 = energy - energy_target1 
        energy_stranding_year1 = energy_excess1[energy_excess1 > 0].index[0]
        energy_target2 = energy_target_uk.loc[asset_code2]
        energy_excess2 = energy - energy_target2
        energy_stranding_year2 = energy_excess2[energy_excess2 > 0].index[0]

        #6. energy costs
        elec_cost = pd.DataFrame(energy_price.range('A34:AH61').value) #electricity price incl. VAT
        elec_cost.set_index(0, inplace = True)
        elec_cost.columns = years
        gas_cost = pd.DataFrame(energy_price.range('A97:AH124').value) #gas price incl. VAT
        gas_cost.set_index(0, inplace = True)
        gas_cost.columns = years
        oil_cost = pd.DataFrame(energy_price.range('A159:AH186').value) #oil price incl. VAT
        oil_cost.set_index(0, inplace = True)
        oil_cost.columns = years
        wood_cost = pd.DataFrame(energy_price.range('A284:AH311').value) #wood pellets price incl. VAT
        wood_cost.set_index(0, inplace = True)
        wood_cost.columns = years
        coal_cost = pd.DataFrame(energy_price.range('A408:AH435').value) #coal price incl. VAT
        coal_cost.set_index(0, inplace = True)
        coal_cost.columns = years
        carbon_cost = pd.DataFrame(energy_price.range('A440:AH467').value) #carbon price incl. VAT
        carbon_cost.set_index(0, inplace = True)
        carbon_cost.columns = years

        total_energy = current_energy

        if 'electricity' in data['MAIN_FUEL'][i]:
            elec_cost = total_energy*elec_cost.loc['UK']*0.7
            gas_cost = total_energy*gas_cost.loc['UK']*0.3
            oil_cost = total_energy*oil_cost.loc['UK']*0
            coal_cost = total_energy*coal_cost.loc['UK']*0
            wood_cost = total_energy*wood_cost.loc['UK']*0
        elif 'gas' in data['MAIN_FUEL'][i]:
            elec_cost = total_energy*elec_cost.loc['UK']*0.3
            gas_cost = total_energy*gas_cost.loc['UK']*0.7
            oil_cost = total_energy*oil_cost.loc['UK']*0
            coal_cost = total_energy*coal_cost.loc['UK']*0
            wood_cost = total_energy*wood_cost.loc['UK']*0
        elif 'oil' in data['MAIN_FUEL'][i]:
            elec_cost = total_energy*elec_cost.loc['UK']*0.5
            gas_cost = total_energy*gas_cost.loc['UK']*0.5
            oil_cost = total_energy*oil_cost.loc['UK']*0.7
            coal_cost = total_energy*coal_cost.loc['UK']*0
            wood_cost = total_energy*wood_cost.loc['UK']*0
        elif 'coal' in data['MAIN_FUEL'][i]:
            elec_cost = total_energy*elec_cost.loc['UK']*0.15
            gas_cost = total_energy*gas_cost.loc['UK']*0.15 
            oil_cost = total_energy*oil_cost.loc['UK']*0
            coal_cost = total_energy*coal_cost.loc['UK']*0.7
            wood_cost = total_energy*wood_cost.loc['UK']*0
        elif 'wood' in data['MAIN_FUEL'][i]:
            elec_cost = total_energy*elec_cost.loc['UK']*0.15
            gas_cost = total_energy*gas_cost.loc['UK']*0.15
            oil_cost = total_energy*oil_cost.loc['UK']*0
            coal_cost = total_energy*coal_cost.loc['UK']*0
            wood_cost = total_energy*wood_cost.loc['UK']*0.7
        else:
            elec_cost = total_energy*elec_cost.loc['UK']*0.5
            gas_cost = total_energy*gas_cost.loc['UK']*0.5
            oil_cost = total_energy*oil_cost.loc['UK']*0
            coal_cost = total_energy*coal_cost.loc['UK']*0
            wood_cost = total_energy*wood_cost.loc['UK']*0

        #7. excess carbon costs and value at risk
        floor_area = data['TOTAL_FLOOR_AREA'][i]
        carbon_price = carbon_cost.loc['UK']
        total_emission = emission * floor_area
        total_target1 = emission_target_uk.loc[asset_code1]* floor_area
        excess_cost1 = carbon_price*(total_emission-total_target1)
        total_target2 = emission_target_uk.loc[asset_code2]* floor_area
        excess_cost2 = carbon_price*(total_emission-total_target2)

        years = list(range(2018,2051))
        costs1 = pd.Series(np.nan, index = years)
        value1 = pd.Series(np.nan, index = years)
        for year in years:
            if excess_cost1[year]<0:
                costs1[year] = 0
                value1[year] = excess_cost1[year]
            else:
                costs1[year] = excess_cost1[year]
                value1[year] = 0

        costs2 = pd.Series(np.nan, index = years)
        value2 = pd.Series(np.nan, index = years)
        for year in years:
            if excess_cost2[year]<0:
                costs2[year] = 0
                value2[year] = excess_cost2[year]
            else:
                costs2[year] = excess_cost2[year]
                value2[year] = 0

        discount_costs1 = costs1.tolist().copy()
        discount_value1 = value1.tolist().copy()
        discount_costs2 = costs2.tolist().copy()
        discount_value2 = value2.tolist().copy()
        for year in list(range(2019,2051)):
            discount_costs1[year-2018] = discount_costs1[year-2018]/(1+data['discount factor'].iloc[0])**(year-2019)
            discount_value1[year-2018] = discount_value1[year-2018]/(1+data['discount factor'].iloc[0])**(year-2019)
            discount_costs2[year-2018] = discount_costs2[year-2018]/(1+data['discount factor'].iloc[0])**(year-2019)
            discount_value2[year-2018] = discount_value2[year-2018]/(1+data['discount factor'].iloc[0])**(year-2019)

        VAR1 = (sum(discount_costs1) + sum(discount_value1))/data['PRICE'][i]
        VAR2 = (sum(discount_costs2) + sum(discount_value2))/data['PRICE'][i]

        #8. retrofit costs
        target1 = emission_target_uk.loc[asset_code1]
        target2 = emission_target_uk.loc[asset_code2]

        Costs1 = pd.Series(index=list(range(2018,2051)))
        Costs2 = pd.Series(index=list(range(2018,2051)))
        for year in years:
            if emission[year] > target1[year]:
                Costs1.iloc[year-2018] = floor_area*AB3/AB4*X2.loc[property_type_code][1]*(AF2.loc['UK'].iloc[0])*(math.exp(AB4*energy[year]/floor_area)-math.exp(AB4*target1[year]*floor_area/(emission[year]*floor_area)*energy[year]/floor_area))*(1-(Z4*(1-target1[year]*floor_area/(emission[year]*floor_area))**2+Z6*(1-target1[year] *floor_area/(emission[year]*floor_area)+Z6)))**(year-2015)
            else:
                Costs1.iloc[year-2018] = 0
            if emission[year] > target2[year]:
                Costs2.iloc[year-2018] = floor_area*AB3/AB4*X2.loc[property_type_code][1]*(AF2.loc['UK'].iloc[0])*(math.exp(AB4*energy[year]/floor_area)-math.exp(AB4*target2[year]*floor_area/(emission[year]*floor_area)*energy[year]/floor_area))*(1-(Z4*(1-target2[year]*floor_area/(emission[year]*floor_area))**2+Z6*(1-target2[year] *floor_area/(emission[year]*floor_area)+Z6)))**(year-2015)
            else:
                Costs2.iloc[year-2018] = 0

        return {
        'building_id': building_id,
        'VAR1': VAR1,
        'VAR2': VAR2,
        'emission (kg/m²)': emission.tolist(),
        'emission stranding year1': emission_stranding_year1,
        'emission_target1': emission_target1.tolist(),
        'emission_excess1': emission_excess1.tolist(),
        'emission stranding year2': emission_stranding_year2,
        'emission_target2': emission_target2.tolist(),
        'emission_excess2': emission_excess2.tolist(),
        'emission_baseline': emission_baseline,
        'energy (kWh/m2)': energy.tolist(),
        'energy stranding year1': energy_stranding_year1,
        'energy_target1': energy_target1.tolist(),
        'energy_excess1': energy_excess1.tolist(),
        'energy stranding year2': energy_stranding_year2,
        'energy_target2': energy_target2.tolist(),
        'energy_excess2': energy_excess2.tolist(),
        'energy_baseline': energy_baseline,
        'elec_cost': elec_cost.tolist(),
        'gas_cost': gas_cost.tolist(),
        'oil_cost': oil_cost.tolist(),
        'coal_cost': coal_cost.tolist(),
        'wood_cost': wood_cost.tolist(),
        'excess carbon costs1': costs1.tolist(),
        'excess carbon savings1': value1.tolist(),
        'excess carbon costs2': costs2.tolist(),
        'excess carbon savings2': value2.tolist(),
        'retrofit costs1': Costs1.tolist(),
        'retrofit costs2': Costs2.tolist(),
        'xaxis': years}
    

if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port='5001')