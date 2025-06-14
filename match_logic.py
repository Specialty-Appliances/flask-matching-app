import pandas as pd
from recordlinkage.preprocessing import clean
import recordlinkage

def match_records_by_fields(df1, df2, min_score=2):
    """
    Match records from df1 (uploaded) to df2 (reference) using fuzzy matching.
    Compares Name, Emails, Address, and Doctors with Jaro-Winkler similarity.
    Returns: df with uploaded data + match scores + matched entity details.
    """

    # Normalize key text fields
    for col in ['Name', 'Emails', 'Address', 'Doctors']:
        df1[col] = clean(df1[col].astype(str).fillna(''))
        df2[col] = clean(df2[col].astype(str).fillna(''))

    # Index candidate pairs using blocking on State
    indexer = recordlinkage.Index()
    indexer.block('State')
    candidate_links = indexer.index(df1, df2)

    # Compare fields using fuzzy matching
    compare = recordlinkage.Compare()
    compare.string('Name', 'Name', method='jarowinkler', threshold=0.85, label='MatchedName')
    compare.string('Emails', 'Emails', method='jarowinkler', threshold=0.85, label='MatchedEmails')
    compare.string('Address', 'Address', method='jarowinkler', threshold=0.85, label='MatchedAddress')
    compare.string('Doctors', 'Doctors', method='jarowinkler', threshold=0.85, label='MatchedDoctors')

    features = compare.compute(candidate_links, df1, df2)

    # Filter by match score threshold
    matches = features[features.sum(axis=1) >= min_score]
    matches['Total_Score'] = matches.sum(axis=1)

    # Merge scores with original uploaded data
    matches_df = matches.reset_index()
    matches_df = matches_df.merge(df1, left_on='level_0', right_index=True)

    # Optional: take best match per ExternalID (de-dupe uploaded)
    matches_df = matches_df.loc[matches_df.groupby('ExternalID')['Total_Score'].idxmax()]

    # Merge in MatchedEntityID and MatchedPracticeName from df2
    df2_subset = df2[['MatchedEntityID', 'Name']].rename(columns={'Name': 'MatchedPracticeName'})
    matches_df = matches_df.merge(df2_subset, left_on='level_1', right_index=True)

    # Final cleanup
    matches_df = matches_df.drop(columns=['level_0', 'level_1'])

    return matches_df
