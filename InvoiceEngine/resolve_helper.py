def resolve_best_match(matches, label):
    """
    Ask user to approve one of the matches.
    
    matches = [
        {"type": "name", "match": "...", "score": 92},
        {"type": "id", "match": "...", "score": 85},
        ...
    ]
    """
    for m in matches:
        print(f"\nPossible {label}: {m['match']} (score={m['score']})")
        reply = input("Use this? (yes/no): ").strip().lower()
        if reply == "yes":
            return m["match"]

    print(f"\nNo valid {label} selected. Aborting.")
    return None
