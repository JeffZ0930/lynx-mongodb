// Q5. New groups
/*
:param [{ personId, minDate }] => { RETURN
  6597069766734 AS personId,
  1288612800000 AS minDate
}
*/
MATCH (person:Person { `id:ID`: $personId })-[:knows*1..2]-(friend)
WHERE
    NOT person=friend
WITH DISTINCT friend
MATCH (friend)<-[membership:hasMember]-(forum)
WHERE
    membership.creationDate > $minDate
WITH
    forum,
    collect(friend) AS friends
OPTIONAL MATCH (friend)<-[:hasCreator]-(post)<-[:containerOf]-(forum)
WHERE
    friend IN friends
WITH
    forum,
    count(post) AS postCount
RETURN
    forum.title AS forumName,
    postCount
ORDER BY
    postCount DESC,
    forum.`id:ID` ASC
LIMIT 20
