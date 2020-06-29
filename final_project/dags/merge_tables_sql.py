sql = """
    MERGE `platinum-trees-277807.bikes_data.cycle_hire` old_
    USING (
        SELECT * FROM `platinum-trees-277807.bikes_data.cycle_hire{{ ds_nodash }}`
    ) new_ ON old_.bike_id = new_.bike_id
            AND old_.start_date = new_.start_date
            AND old_.start_station_id = new_.start_station_id
            AND old_.end_date = new_.end_date
            AND old_.end_station_id = new_.end_station_id
    
    WHEN MATCHED THEN 
        UPDATE SET
            duration = COALESCE(new_.duration, old_.duration)
            ,bike_id = COALESCE(new_.bike_id, old_.bike_id)
            ,end_date = COALESCE(new_.end_date, old_.end_date)
            ,end_station_id = COALESCE(new_.end_station_id, old_.end_station_id)
            ,end_station_name = COALESCE(new_.end_station_name, old_.end_station_name)
            ,start_station_id = COALESCE(new_.start_station_id, old_.start_station_id)
            ,start_station_name = COALESCE(new_.start_station_name, old_.start_station_name)
            # ,end_station_logical_terminal = COALESCE(new_.end_station_logical_terminal, old_.end_station_logical_terminal)
            # ,start_station_logical_terminal = COALESCE(new_.start_station_logical_terminal, old_.start_station_logical_terminal)
            # ,end_station_priority_id = COALESCE(new_.end_station_priority_id, old_.end_station_priority_id)
   
   -- if we have NEW data, which are not available in OLD table
   -- WHEN NOT MATCHED BY TARGET AND DATE(new_.start_date) = '{{ ds }}' THEN
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            rental_id
            ,duration
            ,bike_id
            ,end_date
            ,end_station_id
            ,end_station_name
            ,start_date
            ,start_station_id
            ,start_station_name
            ,end_station_logical_terminal
            ,start_station_logical_terminal
            ,end_station_priority_id
        )
        VALUES (
            new_.rental_id
            ,new_.duration
            ,new_.bike_id
            ,new_.end_date
            ,new_.end_station_id
            ,new_.end_station_name
            ,new_.start_date
            ,new_.start_station_id
            ,new_.start_station_name
            ,NULL
            ,NULL
            ,NULL
        )
        
    -- if we have OLD data, which are not available in NEW table
    -- WHEN NOT MATCHED BY SOURCE AND DATE(new_.start_date) = '{{ ds }}' THEN
    --    DELETE 
"""