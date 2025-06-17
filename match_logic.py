import pandas as pd
from recordlinkage.preprocessing import clean
import recordlinkage
from config import STATE_LOOKUP
def match_records_by_fields(df1, df2, min_score=2):
    """
    Finds matching records between two DataFrames using fuzzy logic.
    Compares Name, Emails, Address, and Doctors with Jaro-Winkler similarity.
    Blocking is done on State to reduce candidate comparisons.
    """

    if 'SourceID' not in df1.columns:
        df1 = df1.copy()
        df1['SourceID'] = df1.index.astype(str)

    original_df1 = df1.copy()

    df1_to_match = df1.loc[:, ~df1.columns.duplicated()].copy()
    df2 = df2.loc[:, ~df2.columns.duplicated()].copy()

    print(" Parsed columns:", list(df1_to_match.columns))
    print(" First row sample:", df1_to_match[['Name', 'Address', 'Emails', 'Doctors', 'Zip']].head(1).to_dict(orient='records'))

    # Clean fields
    for col in ['Name', 'Emails', 'Address', 'Doctors', 'State']:
        if col in df1_to_match.columns:
            df1_to_match[col] = clean(df1_to_match[col].astype(str).fillna(''))
        if col in df2.columns:
            df2[col] = clean(df2[col].astype(str).fillna(''))

    # Normalize state names from full name → abbreviation
    if 'State' in df1_to_match.columns:
        df1_to_match['State'] = df1_to_match['State'].str.strip().map(lambda x: STATE_LOOKUP.get(x.lower(), x.upper()))

    if 'State' in df2.columns:
        df2['State'] = df2['State'].str.strip().str.upper()

    # Blocking
    indexer = recordlinkage.Index()
    if 'State' in df1_to_match.columns and 'State' in df2.columns:
        indexer.block('State')
    else:
        indexer.full()

    candidate_links = indexer.index(df1_to_match, df2)

    empty_result_cols = {
        'MatchedName': 0.0, 'MatchedEmails': 0.0, 'MatchedAddress': 0.0,
        'MatchedDoctors': 0.0, 'TotalScore': 0.0, 'MatchedEntityID': '',
        'MatchedPracticeName': ''
    }

    if len(candidate_links) == 0:
        print(" No candidate pairs found.")
        for col, val in empty_result_cols.items():
            original_df1[col] = val
        return original_df1

    # Compare
    compare = recordlinkage.Compare()
    compare.string('Name', 'Name', method='jarowinkler', threshold=0.85, label='MatchedName')
    compare.string('Emails', 'Emails', method='jarowinkler', threshold=0.85, label='MatchedEmails')
    compare.string('Address', 'Address', method='jarowinkler', threshold=0.85, label='MatchedAddress')
    compare.string('Doctors', 'Doctors', method='jarowinkler', threshold=0.85, label='MatchedDoctors')

    features = compare.compute(candidate_links, df1_to_match, df2)

    matches = features[features.sum(axis=1) >= min_score].copy()
    if matches.empty:
        print(" No matches above threshold.")
        for col, val in empty_result_cols.items():
            original_df1[col] = val
        return original_df1

    matches['TotalScore'] = matches.sum(axis=1)
    matches_df = matches.reset_index()
    matches_df = matches_df.merge(df1_to_match[['SourceID']], left_on='level_0', right_index=True)

    #  FIX: Properly map matched IDs and practice names
    df2 = df2.reset_index(drop=True)
    df2['level_1'] = df2.index
    df2_subset = df2[['level_1', 'MatchedEntityID', 'Name']].rename(columns={'Name': 'MatchedPracticeName'})

    best_matches = matches_df.loc[matches_df.groupby('SourceID')['TotalScore'].idxmax()]
    best_matches_with_names = best_matches.merge(df2_subset, on='level_1', how='left')

    result_columns = [
        'SourceID', 'MatchedName', 'MatchedEmails', 'MatchedAddress',
        'MatchedDoctors', 'TotalScore', 'MatchedEntityID', 'MatchedPracticeName'
    ]
    match_results = best_matches_with_names[result_columns]

    final_df = pd.merge(original_df1, match_results, on='SourceID', how='left')

    for col in ['MatchedName', 'MatchedEmails', 'MatchedAddress', 'MatchedDoctors', 'TotalScore']:
        final_df[col] = final_df[col].fillna(0.0).astype(float)

    for col in ['MatchedEntityID', 'MatchedPracticeName']:
        final_df[col] = final_df[col].fillna('')

    # Log top 5 for review
    print(" Top 5 Matches:")
    print(final_df[['SourceID', 'MatchedPracticeName', 'MatchedEntityID', 'TotalScore']].head())

    return final_df
