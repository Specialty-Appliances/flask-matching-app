import pandas as pd
from recordlinkage.preprocessing import clean
import recordlinkage


def match_records_by_fields(df1, df2, min_score=2):
    """
    Finds matching records between two DataFrames based on multiple fields.
    This version returns ALL records from the original file (df1), showing match
    scores for those that found a match, and empty/zero scores for those that did not.

    Args:
        df1 (pd.DataFrame): The source DataFrame (e.g., from an uploaded file).
                            Must contain 'ExternalID' and the fields to be matched.
        df2 (pd.DataFrame): The target DataFrame to match against (e.g., from the datalake).
        min_score (int): The minimum number of fields that must match for a record pair to be considered a match.

    Returns:
        pd.DataFrame: A DataFrame containing all original rows from df1, augmented with match information.
    """
    # Create a copy of the original df1 to preserve it for the final merge
    original_df1 = df1.copy()

    # Make a copy of df1 to modify for matching, to avoid SettingWithCopyWarning
    df1_to_match = df1.copy()

    # --- CORRECTED: Added 'State' to the list of columns to be cleaned. ---
    # This ensures the blocking on 'State' works correctly.
    for col in ['Name', 'Emails', 'Address', 'Doctors', 'State']:
        if col in df1_to_match.columns:
            df1_to_match[col] = clean(df1_to_match[col].astype(str).fillna(''))
        if col in df2.columns:
            df2[col] = clean(df2[col].astype(str).fillna(''))

    # Index records on 'State' to reduce comparison space
    indexer = recordlinkage.Index()
    indexer.block('State')
    candidate_links = indexer.index(df1_to_match, df2)

    # Define columns for the case where no matches are found at all
    empty_result_cols = {
        'MatchedName': 0.0, 'MatchedEmails': 0.0, 'MatchedAddress': 0.0,
        'MatchedDoctors': 0.0, 'Total_Score': 0.0, 'MatchedEntityID': '',
        'MatchedPracticeName': ''
    }

    # If no potential candidates are found, return the original data with empty score columns
    if len(candidate_links) == 0:
        for col, val in empty_result_cols.items():
            original_df1[col] = val
        return original_df1

    # Define comparison rules for matching
    compare = recordlinkage.Compare()
    compare.string('Name', 'Name', method='jarowinkler', threshold=0.85, label='MatchedName')
    compare.string('Emails', 'Emails', method='jarowinkler', threshold=0.85, label='MatchedEmails')
    compare.string('Address', 'Address', method='jarowinkler', threshold=0.85, label='MatchedAddress')
    compare.string('Doctors', 'Doctors', method='jarowinkler', threshold=0.85, label='MatchedDoctors')

    # Compute features and filter for potential matches
    features = compare.compute(candidate_links, df1_to_match, df2)
    matches = features[features.sum(axis=1) >= min_score].copy()

    # If no records meet the minimum score, return original data with empty score columns
    if matches.empty:
        for col, val in empty_result_cols.items():
            original_df1[col] = val
        return original_df1

    matches['Total_Score'] = matches.sum(axis=1)

    matches_df = matches.reset_index()
    # Merge with just the ExternalID from the matching df1 to link back
    matches_df = matches_df.merge(df1_to_match[['ExternalID']], left_on='level_0', right_index=True)

    # **Critical Step**: Keep only the best-scoring match for each unique source record
    best_matches = matches_df.loc[matches_df.groupby('ExternalID')['Total_Score'].idxmax()]

    # Merge with df2 to get the name and ID of the matched entity
    df2_subset = df2[['MatchedEntityID', 'Name']].rename(columns={'Name': 'MatchedPracticeName'})
    best_matches_with_names = best_matches.merge(df2_subset, left_on='level_1', right_index=True)

    # Select only the new columns we want to add to the original dataframe
    result_columns = [
        'ExternalID', 'MatchedName', 'MatchedEmails', 'MatchedAddress',
        'MatchedDoctors', 'Total_Score', 'MatchedEntityID', 'MatchedPracticeName'
    ]
    match_results = best_matches_with_names[result_columns]

    # --- FINAL STEP: Perform a LEFT JOIN from the original df1 to the match results ---
    final_df = pd.merge(original_df1, match_results, on='ExternalID', how='left')

    # Fill NaN values for non-matched rows with appropriate defaults
    score_cols = ['MatchedName', 'MatchedEmails', 'MatchedAddress', 'MatchedDoctors', 'Total_Score']
    for col in score_cols:
        if col in final_df.columns:
            final_df[col] = final_df[col].fillna(0.0).astype(float)

    string_cols = ['MatchedEntityID', 'MatchedPracticeName']
    for col in string_cols:
        if col in final_df.columns:
            final_df[col] = final_df[col].fillna('')

    return final_df
