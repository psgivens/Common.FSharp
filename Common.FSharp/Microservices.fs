module Common.FSharp.Suave

open Suave
open Suave.Operators

open Newtonsoft.Json
open Newtonsoft.Json.Serialization

let inline noMatch (ctx:HttpContext) = async.Return Option<HttpContext>.None

let toJson v =
  let jsonSerializerSettings = JsonSerializerSettings()
  jsonSerializerSettings.ContractResolver <- CamelCasePropertyNamesContractResolver ()
  JsonConvert.SerializeObject(v, jsonSerializerSettings)

let fromJson<'a> json =
  JsonConvert.DeserializeObject(json, typeof<'a>) :?> 'a

let getDtoFromReq<'a> (req : HttpRequest) =
  let getString (rawForm:byte []) =
    System.Text.Encoding.UTF8.GetString rawForm
  req.rawForm |> getString |> fromJson<'a>

let restful (processRequest:'a -> WebPart) :WebPart= 
  Writers.setMimeType "application/json; charset=utf-8"
  >=> request (getDtoFromReq >> processRequest)

let restfulPathScan (processRequest:'a -> 'b -> WebPart) pathArgs :WebPart = 
  Writers.setMimeType "application/json; charset=utf-8"
  >=> request (getDtoFromReq >> (processRequest pathArgs))

