

SELECT 
    m.principal_id,
    u.user_name,
    u.display_name,
    g.group_name
FROM system.information_schema.account_group_members m
JOIN system.information_schema.account_users u
    ON m.principal_id = u.user_id
JOIN system.information_schema.account_groups g
    ON m.group_id = g.group_id
ORDER BY g.group_name, u.user_name;
