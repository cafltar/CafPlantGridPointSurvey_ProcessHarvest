from importlib.machinery import SourceFileLoader
cafcore_qc_0_1_4 = SourceFileLoader('qc', '../../CafLogisticsCorePythonLibrary/CafCore/cafcore/qc.py').load_module()


def calculate_p2(df, args):
    df_p2a0 = df.copy()
    
    harvest_areas = args['harvest_areas']

    df_p2a0['SeedYieldAirDry_P2'] = df_p2a0.apply(
        lambda row: row['SeedMassAirDry_P1'] / harvest_areas[row['HarvestYear']], axis = 1)
    df_p2a0['SeedYieldOvenDry_P2'] = df_p2a0.apply(
        lambda row: row['SeedMassOvenDry_P1'] / harvest_areas[row['HarvestYear']], axis = 1)
    df_p2a0['BiomassAirDryPerArea_P2'] = df_p2a0.apply(
        lambda row: row['BiomassAirDry_P1'] / harvest_areas[row['HarvestYear']], axis = 1)
    df_p2a0['ResidueMassAirDryPerArea_P2'] = df_p2a0.apply(
        lambda row: (row['BiomassAirDry_P1'] - row['SeedMassAirDry_P1']) / harvest_areas[row['HarvestYear']], axis = 1)

    p2_cols = ['SeedYieldAirDry_P2', 'SeedYieldOvenDry_P2', 'BiomassAirDryPerArea_P2', 'ResidueMassAirDryPerArea_P2']
    omit_cols = [col for col in df_p2a0.columns if col not in p2_cols]
    df_p2a1 = cafcore_qc_0_1_4.initialize_qc(df_p2a0, omit_cols)
    
    df_p2a1 = cafcore_qc_0_1_4.quality_assurance_calculated(df_p2a1, 'SeedYieldAirDry_P2', ['SeedMassAirDry_P1'])
    df_p2a1 = cafcore_qc_0_1_4.quality_assurance_calculated(df_p2a1, 'SeedYieldOvenDry_P2', ['SeedMassOvenDry_P1'])
    df_p2a1 = cafcore_qc_0_1_4.quality_assurance_calculated(df_p2a1, 'BiomassAirDryPerArea_P2', ['BiomassAirDry_P1'])
    df_p2a1 = cafcore_qc_0_1_4.quality_assurance_calculated(df_p2a1, 'ResidueMassAirDryPerArea_P2', ['BiomassAirDry_P1', 'SeedMassAirDry_P1'])

    return df_p2a1