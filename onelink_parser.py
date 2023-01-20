import helpers as h


def main():
    onelinks = h.get_onelinks()
    if len(onelinks) == 0:
        print("No new onelinks found")
        return True

    print(f"{len(onelinks)} new onelinks found")

    result_dict = {}
    print('Parsing onelinks...')
    for i in onelinks:
        result_dict[i] = h.parse_unique_onelinks(i)

    df = h.convert_to_dataframe(result_dict)
    onelinks_tbl = h.get_current_onelinks_table()
    df = h.ensure_column_consistency(df)
    upload_this_table_to_snowflake = h.concat_onelink_tables(onelinks_tbl, df)
    upload_this_table_to_snowflake = h.catch_stray_commas(upload_this_table_to_snowflake)
    return h.push_to_snowflake(upload_this_table_to_snowflake)


if __name__ == "__main__":
    main()
