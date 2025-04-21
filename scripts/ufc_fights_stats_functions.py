from bs4 import BeautifulSoup
import requests
import pandas as pd

def parse_overall_totals(tbody, event_name, fight_id):
    """
    Given the <tbody> that corresponds to overall 'Totals',
    return a list of tuples (one tuple per fighter).
    Tuple format:
      (Fighter, KD, Sig. Str, Sig. Str %, Total Str, Td, Td %, Sub. att, Rev, Ctrl)
    """
    # There's typically one <tr> with 2 fighters stacked in <p> tags
    row = tbody.find('tr', class_='b-fight-details__table-row')
    cols = row.find_all('td', class_='b-fight-details__table-col')
    
    # First col has the fighter names in <p> elements
    fighter_names = [p.get_text(strip=True) for p in cols[0].find_all('p')]

    # Then each subsequent col has 2 lines of data (one per fighter)
    data_per_column = []
    for c in cols[1:]:
        lines = [p.get_text(strip=True) for p in c.find_all('p')]
        data_per_column.append(lines)

    # Build a list of tuples
    results = []
    for i in range(len(fighter_names)):
        # For each fighter i, build a tuple from fighter + each column
        fighter_tuple = (
            event_name,
            fight_id,
            fighter_names[i],
            data_per_column[0][i],  # KD
            data_per_column[1][i],  # Sig. Str
            data_per_column[2][i],  # Sig. Str %
            data_per_column[3][i],  # Total Str
            data_per_column[4][i],  # Td
            data_per_column[5][i],  # Td %
            data_per_column[6][i],  # Sub. att
            data_per_column[7][i],  # Rev.
            data_per_column[8][i]   # Ctrl
        )
        results.append(fighter_tuple)

    return results



def parse_significant_strikes_overall(tbody, event_name, fight_id):
    """
    Similar approach: each fighter is in the same <tr>, 
    the first <td> has Fighter name in 2 <p> tags,
    the rest have 2 lines of data each.
    Returns a list of tuples:
       (Fighter, Sig. Str, Sig. Str %, Sig. Head, Sig. Body, 
        Sig. Leg, Sig. Distance, Sig. Clinch, Sig. Ground)
    """
    row = tbody.find('tr', class_='b-fight-details__table-row')
    cols = row.find_all('td', class_='b-fight-details__table-col')
    
    # Fighter names
    fighter_names = [p.get_text(strip=True) for p in cols[0].find_all('p')]
    
    # Remaining columns
    data_per_column = []
    for c in cols[1:]:
        lines = [p.get_text(strip=True) for p in c.find_all('p')]
        data_per_column.append(lines)
        
    results = []
    for i in range(len(fighter_names)):
        # Build a tuple for each fighter
        fighter_tuple = (
            event_name,
            fight_id,
            fighter_names[i],          # Fighter
            # data_per_column[0][i],     # Sig. Str
            # data_per_column[1][i],     # Sig. Str %
            data_per_column[2][i],     # Sig. Head
            data_per_column[3][i],     # Sig. Body
            data_per_column[4][i],     # Sig. Leg
            data_per_column[5][i],     # Sig. Distance
            data_per_column[6][i],     # Sig. Clinch
            data_per_column[7][i],     # Sig. Ground
        )
        results.append(fighter_tuple)

    return results



def parse_totals_by_round(tbody, event_name, fight_id):
    """
    Parse the 'Totals by Round' table body, returning a list of tuples:
      (Round, Fighter, KD, Sig. Str, Sig. Str %, Total Str, Td, Td %, Sub. att, Rev., Ctrl)
    """
    rounds_data = []
    
    # Each round heading is in a <thead>, followed by a <tr>
    all_round_headers = tbody.find_all('th', text=lambda x: x and "Round" in x)
    
    for header in all_round_headers:
        round_name = header.get_text(strip=True)  # e.g. 'Round 1'
        
        # Move to the parent <thead>, then find the next <tr>
        thead = header.find_parent('thead')
        tr = thead.find_next_sibling('tr')
        
        cols = tr.find_all('td', class_='b-fight-details__table-col')
        
        # Fighter names
        fighter_names = [p.get_text(strip=True) for p in cols[0].find_all('p')]
        
        # Next columns (KD, Sig. Str, Sig. Str %, ...)
        data_per_column = []
        for c in cols[1:]:
            lines = [p.get_text(strip=True) for p in c.find_all('p')]
            data_per_column.append(lines)
        
        
        for i, fighter in enumerate(fighter_names):
            round_tuple = (
                event_name,
                fight_id,
                round_name,                    # Round
                fighter,                       # Fighter
                data_per_column[0][i],         # KD
                data_per_column[1][i],         # Sig. Str
                data_per_column[2][i],         # Sig. Str %
                data_per_column[3][i],         # Total Str
                data_per_column[4][i],         # Td
                data_per_column[5][i],         # Td %
                data_per_column[6][i],         # Sub. att
                data_per_column[7][i],         # Rev
                data_per_column[8][i]          # Ctrl
            )
            rounds_data.append(round_tuple)
    
    return rounds_data



def parse_significant_by_round(tbody, event_name, fight_id):
    """
    Parse the 'Significant Strikes by Round' table body,
    returning a list of tuples:
      (Round, Fighter, Sig. Str, Sig. Str %, Sig. Head, Sig. Body, 
       Sig. Leg, Sig. Distance, Sig. Clinch, Sig. Ground)
    """
    rounds_data = []
    all_round_headers = tbody.find_all('th', text=lambda x: x and "Round" in x)
    
    for header in all_round_headers:
        round_name = header.get_text(strip=True)  # 'Round 1', 'Round 2', ...
        thead = header.find_parent('thead')
        tr = thead.find_next_sibling('tr')
        
        cols = tr.find_all('td', class_='b-fight-details__table-col')
        
        # Fighter names
        fighter_names = [p.get_text(strip=True) for p in cols[0].find_all('p')]
        
        # Next columns for significant strikes
        data_per_column = []
        for c in cols[1:]:
            lines = [p.get_text(strip=True) for p in c.find_all('p')]
            data_per_column.append(lines)
        
        for i, fighter in enumerate(fighter_names):
            round_tuple = (
                event_name,
                fight_id,
                round_name,                # Round
                fighter,                   # Fighter
                # data_per_column[0][i],     # Sig. Str
                # data_per_column[1][i],     # Sig. Str %
                data_per_column[2][i],     # Sig. Head
                data_per_column[3][i],     # Sig. Body
                data_per_column[4][i],     # Sig. Leg
                data_per_column[5][i],     # Sig. Distance
                data_per_column[6][i],     # Sig. Clinch
                data_per_column[7][i]      # Sig. Ground
            )
            rounds_data.append(round_tuple)
    
    return rounds_data




