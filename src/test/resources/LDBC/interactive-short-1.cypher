// IS1. Profile of a person
/*
:param personId: 10995116277794
 */

MATCH (n:Person {`_id`: $personId }) -[:isLocatedIn]-> (p:Place)
RETURN
    n.firstName AS firstName,
    n.lastName AS lastName,
    n.birthday AS birthday,
    n.locationIP AS locationIP,
    n.browserUsed AS browserUsed,
    p.`_id` AS cityId,
    n.gender AS gender,
    n.creationDate AS creationDat

//MATCH (n:Person {`firstName`: $firstName }) -[:isLocatedIn]-> (p:Place)
//RETURN
//  n.firstName AS firstName,
//  n.lastName AS lastName,
//  p.`_id` AS cityId

//MATCH (n:Person {`id:ID`: $personId }) -[:isLocatedIn]-> (p:Place) <-[:isLocatedIn]- (n2:Person)
//RETURN
//    n2.firstName AS firstName,
//    n2.lastName AS lastName,
//    n2.birthday AS birthday,
//    n2.locationIP AS locationIP,
//    n2.browserUsed AS browserUsed,
//    p.`id:ID` AS placeId,
//    p.name AS placeName,
//    n2.gender AS gender,
//    n2.creationDate AS creationDate

//MATCH (p:Place {`_id`: $placeId}) <-[:isLocatedIn]- (n:Person)
//RETURN
//  n.`_id` AS ID,
//  n.firstName AS firstName,
//  n.lastName AS LastName

//MATCH (n:Person) -[:isLocatedIn]-> (p:Place {`_id`: $placeId})
//RETURN *

//MATCH (p1:Person {`_id`: $personId }) <-[:knows]- (p2:Person)
//RETURN
//  p2.`_id` AS ID,
//  p2.firstName AS firstName,
//  p2.lastName AS lastName,
//  p2.birthday AS birthday



