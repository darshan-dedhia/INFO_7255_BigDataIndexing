package edu.info7225.darshandedhia.bigdataindexing.api;

import edu.info7225.darshandedhia.bigdataindexing.Exceptions.InternalServerErrorException;
import edu.info7225.darshandedhia.bigdataindexing.Exceptions.ObjectNotFoundException;
import edu.info7225.darshandedhia.bigdataindexing.service.JsonService;
import edu.info7225.darshandedhia.bigdataindexing.utils.MD5Utils;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;

import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Set;

@Controller
@RequestMapping(produces = "application/json")
public class RestController extends API{

    /**
     *
     */
    private Jedis cache = new Jedis();
    
    @Autowired(required = false)
    JsonService jsonService;

    /**
     *
     *
     * @param body
     * @param headers
     * @return
     */
    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity save(@RequestBody String body,
                               @RequestHeader Map<String, String> headers) {
        String ETag = null;
        try {
            JSONObject jsonObject = validateSchema(body);
            System.out.println(jsonObject);
            String objType = jsonObject.getString("objectType");
            System.out.println(objType);
            String objID = jsonObject.getString("objectId");
            System.out.println(objID);
            
            if (cache.get(getKey(objType, objID)) != null) {
                System.out.println("In error");
                throw new InternalServerErrorException(alreadyExistsMessage);
            }
            System.out.println("Outta error");
            String key = this.jsonService.savePlan(jsonObject, objType);
            System.out.println(key);
            JSONObject plan = this.jsonService.getPlan(key);
            System.out.println(plan);
            if (plan == null) {
                throw new ObjectNotFoundException("Unable to get plan");
            }
            
            ETag = MD5Utils.hashString(plan.toString());
            String ETagKey = getETagKey(objType, objID);
            cache.set(ETagKey, ETag);

        } catch (JSONException | ValidationException e) {
            return badRequest(e.getMessage());
        } catch (NoSuchAlgorithmException e) {
            return internalServerError(e.getMessage());
        } catch (InternalServerErrorException e) {
            return conflict(e.getMessage());
        } catch (Exception e) {
            return internalServerError(e.getMessage());
        }

        return created(createdMessage, ETag);

    }

    /**
     *
     * @param objectType
     * @param objectId
     * @param ifMatch
     * @return
     */
    @RequestMapping(value = "/{object}/{id}", method = RequestMethod.GET, headers = "If-Match")
    @ResponseBody
    public ResponseEntity getJsonIfMatch(@PathVariable("object") String objectType,
                                         @PathVariable("id") String objectId,
                                         @RequestHeader(name = HttpHeaders.IF_MATCH) String ifMatch) {
    	JSONObject jsonObject = null;
    	try {

            String hashString = getEtagString(objectType, objectId);

            if (hashString.equals(ifMatch)) {
            	jsonObject = this.jsonService.getPlan(getKey(objectType, objectId));
            	if (jsonObject == null || jsonObject.isEmpty()) {
                    throw new ObjectNotFoundException(objectNotFoundMessage);
                }
                return ok(jsonObject.toString(), hashString);
            } else {
                return notModified(null, hashString);
            }

        } catch (ObjectNotFoundException ex) {
            return notFound(ex.getMessage());
        } catch (JSONException ex) {
            return badRequest(ex.getMessage());
        } catch (Exception e) {
            return internalServerError(e.getMessage());
        }
    }

    /**
     *
     * @param objectType
     * @param objectId
     * @return
     */
    @RequestMapping(value = "/{object}/{id}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public ResponseEntity getJson(@PathVariable("object") String objectType,
                                  @PathVariable("id") String objectId) {
        String ETag = null;
        JSONObject jsonObject = null;
        try {
            jsonObject = this.jsonService.getPlan(getKey(objectType, objectId));
            if (jsonObject == null || jsonObject.isEmpty()) {
                throw new ObjectNotFoundException(objectNotFoundMessage);
            }
            ETag = getEtagString(objectType, objectId);

        } catch (JSONException ex) {
            return badRequest(ex.getMessage());
        } catch (ObjectNotFoundException ex) {
            return notFound(ex.getMessage());
        } catch (Exception e) {
            return internalServerError(e.getMessage());
        }
        return ok(jsonObject.toString(), ETag);

    }

    /**
     *
     * @param objectType
     * @param objectId
     * @param ifNoneMatch
     * @return
     */
    @RequestMapping(value = "/{object}/{id}", method = RequestMethod.GET, headers = "If-None-Match")
    @ResponseBody
    public ResponseEntity getJsonIfNoneMatch(@PathVariable("object") String objectType,
                                             @PathVariable("id") String objectId,
                                             @RequestHeader(name = HttpHeaders.IF_NONE_MATCH) String ifNoneMatch) {
    	JSONObject jsonObject = null;
        try {

            String hashString = getEtagString(objectType, objectId);

            if (!hashString.equals(ifNoneMatch)) {
                jsonObject = this.jsonService.getPlan(getKey(objectType, objectId));
                if (jsonObject == null || jsonObject.isEmpty()) {
                    throw new ObjectNotFoundException(objectNotFoundMessage);
                }
                return ok(jsonObject.toString(), hashString);
            } else {
                return notModified(null, hashString);
            }

        } catch (ObjectNotFoundException ex) {
            return notFound(ex.getMessage());
        } catch (JSONException ex) {
            return badRequest(ex.getMessage());
        } catch (Exception e) {
            return internalServerError(e.getMessage());
        }
    }
    
    
    /**
    *
    * @param objectType
    * @param objectId
    * @return
    */
   @RequestMapping(value = "/{object}/{id}", method = RequestMethod.PATCH, produces = "application/json")
   @ResponseBody
   public ResponseEntity patchJson(@PathVariable("object") String objectType,
           @PathVariable("id") String objectId, @RequestBody String body) {
       String ETag = null;
       JSONObject jsonObject = null;
       String resultJsonString = null;
       try {

           JSONObject bodyJson = validateSchema(body);

           jsonObject = this.jsonService.mergeJson(bodyJson, getKey(objectType, objectId));
           if (jsonObject == null || jsonObject.isEmpty()) {
               throw new ObjectNotFoundException(objectNotFoundMessage);
           }
           
           resultJsonString = jsonObject.toString();
           String keyString = this.jsonService.updatePlan(jsonObject, (String)jsonObject.get("objectType"));
           
           if (keyString == null || keyString.isEmpty()) {
               throw new ObjectNotFoundException(objectNotFoundMessage);
           }
            
            
           ETag = MD5Utils.hashString(resultJsonString);
           String ETagKey = getETagKey(objectType, objectId);
           cache.set(ETagKey, ETag);

       } catch (JSONException ex) {
           return badRequest(ex.getMessage());
       } catch (ObjectNotFoundException ex) {
           return notFound(ex.getMessage());
       } catch (Exception e) {
           return internalServerError(e.getMessage());
       }
       return ok(resultJsonString, ETag);

   }
   
   /**
    *
    * @param objectType
    * @param objectId
    * @return
    */
   @RequestMapping(value = "/{object}/{id}", method = RequestMethod.PATCH, produces = "application/json", headers = "If-None-Match")
   @ResponseBody
   public ResponseEntity patchJsonIfNoneMatch(@PathVariable("object") String objectType,
           @PathVariable("id") String objectId, @RequestBody String body,
           @RequestHeader(name = HttpHeaders.IF_NONE_MATCH) String ifNoneMatch) {
       String ETag = null;
       JSONObject jsonObject = null;
       String resultJsonString = null;
       try {
           
           String hashString = getEtagString(objectType, objectId);
           if(hashString.equals(ifNoneMatch)){
               return notModified(null, hashString);
           }
           JSONObject bodyJson = validateSchema(body);

           jsonObject = this.jsonService.mergeJson(bodyJson, getKey(objectType, objectId));
           if (jsonObject == null || jsonObject.isEmpty()) {
               throw new ObjectNotFoundException(objectNotFoundMessage);
           }
           
            resultJsonString = jsonObject.toString();
           String keyString = this.jsonService.updatePlan(jsonObject, (String)jsonObject.get("objectType"));
           
           if (keyString == null || keyString.isEmpty()) {
               throw new ObjectNotFoundException(objectNotFoundMessage);
           }
           
           ETag = MD5Utils.hashString(resultJsonString);
           String ETagKey = getETagKey(objectType, objectId);
           cache.set(ETagKey, ETag);

       } catch (JSONException ex) {
           return badRequest(ex.getMessage());
       } catch (ObjectNotFoundException ex) {
           return notFound(ex.getMessage());
       } catch (Exception e) {
           return internalServerError(e.getMessage());
       }
       return ok(resultJsonString, ETag);

   }
   
   /**
    *
    * @param objectType
    * @param objectId
    * @return
    */
   @RequestMapping(value = "/{object}/{id}", method = RequestMethod.PATCH, produces = "application/json", headers = "If-Match")
   @ResponseBody
   public ResponseEntity patchJsonIfMatch(@PathVariable("object") String objectType,
           @PathVariable("id") String objectId, @RequestBody String body,
           @RequestHeader(name = HttpHeaders.IF_MATCH) String ifMatch) {
       String ETag = null;
       JSONObject jsonObject = null;
       String resultJsonString = null;
       try {
           
           String hashString = getEtagString(objectType, objectId);
           if(!hashString.equals(ifMatch)){
               return notModified(null, hashString);
           }
           JSONObject bodyJson = validateSchema(body);

           jsonObject = this.jsonService.mergeJson(bodyJson, getKey(objectType, objectId));
           if (jsonObject == null || jsonObject.isEmpty()) {
               throw new ObjectNotFoundException(objectNotFoundMessage);
           }
           resultJsonString = jsonObject.toString();
           String keyString = this.jsonService.updatePlan(jsonObject, (String)jsonObject.get("objectType"));
           
           if (keyString == null || keyString.isEmpty()) {
               throw new ObjectNotFoundException(objectNotFoundMessage);
           }
           
           ETag = MD5Utils.hashString(resultJsonString);
           String ETagKey = getETagKey(objectType, objectId);
           cache.set(ETagKey, ETag);

       } catch (JSONException ex) {
           return badRequest(ex.getMessage());
       } catch (ObjectNotFoundException ex) {
           return notFound(ex.getMessage());
       } catch (Exception e) {
           return internalServerError(e.getMessage());
       }
       return ok(resultJsonString, ETag);

   }

   /**
    *
    * @param objectType
    * @param objectId
    * @return
    */
   @RequestMapping(value = "/{object}/{id}", method = RequestMethod.PUT, produces = "application/json")
   @ResponseBody
   public ResponseEntity putJson(@PathVariable("object") String objectType,
           @PathVariable("id") String objectId, @RequestBody String body) {
       String ETag = null;
       String key = null;
       JSONObject plan = null;
       try {

           //validate schema
           JSONObject bodyJson = validateSchema(body);

           //update object and get key
           key = this.jsonService.updatePlan(bodyJson, (String) bodyJson.get("objectType"));
           if (key == null || key.isEmpty()) {
               throw new ObjectNotFoundException(objectNotFoundMessage + "-");
           }

           //Get saved obj on based of key
           plan = this.jsonService.getPlan(key);
           if (plan == null || plan.isEmpty()) {
               throw new ObjectNotFoundException(objectNotFoundMessage);
           }
           

           ETag = MD5Utils.hashString(plan.toString());
           String ETagKey = getETagKey(objectType, objectId);
           cache.set(ETagKey, ETag);

       } catch (JSONException ex) {
           return badRequest(ex.getMessage());
       } catch (Exception e) {
           return internalServerError(e.getMessage());
       }
       return ok(plan.toString(), ETag);

   }
   
   /**
    *
    * @param objectType
    * @param objectId
    * @return
    */
   @RequestMapping(value = "/{object}/{id}", method = RequestMethod.PUT, produces = "application/json", headers = "If-None-Match")
   @ResponseBody
   public ResponseEntity putJsonIfNoneMatch(@PathVariable("object") String objectType,
           @PathVariable("id") String objectId, @RequestBody String body,
           @RequestHeader(name = HttpHeaders.IF_NONE_MATCH) String ifNoneMatch) {
       
       String ETag = null;
       String key = null;
       JSONObject plan = null;

       try {
           
           String hashString = getEtagString(objectType, objectId);
           if(hashString.equals(ifNoneMatch)){
               return notModified(null, hashString);
           }

           //validate schema
           JSONObject bodyJson = validateSchema(body);

           //update object and get key
           key = this.jsonService.updatePlan(bodyJson, (String) bodyJson.get("objectType"));
           if (key == null || key.isEmpty()) {
               throw new ObjectNotFoundException(objectNotFoundMessage);
           }

           //Get saved obj on based of key
           plan = this.jsonService.getPlan(key);
           if (plan == null || plan.isEmpty()) {
               throw new ObjectNotFoundException(objectNotFoundMessage);
           } 
   

           ETag = MD5Utils.hashString(plan.toString());
           String ETagKey = getETagKey(objectType, objectId);
           cache.set(ETagKey, ETag);

       } catch (JSONException ex) {
           return badRequest(ex.getMessage());
       } catch (Exception e) {
           return internalServerError(e.getMessage());
       }
       return ok(plan.toString(), ETag);

   }
   
   /**
    *
    * @param objectType
    * @param objectId
    * @return
    */
   @RequestMapping(value = "/{object}/{id}", method = RequestMethod.PUT, produces = "application/json", headers = "If-Match")
   @ResponseBody
   public ResponseEntity putJsonIfMatch(@PathVariable("object") String objectType,
           @PathVariable("id") String objectId, @RequestBody String body,
           @RequestHeader(name = HttpHeaders.IF_MATCH) String ifMatch) {
       
       String ETag = null;
       String key = null;
       JSONObject plan = null;
       
       try {
           
           String hashString = getEtagString(objectType, objectId);
           if(!hashString.equals(ifMatch)){
               return notModified(null, hashString);
           }

           //validate schema
           JSONObject bodyJson = validateSchema(body);

           //update object and get key
           key = this.jsonService.updatePlan(bodyJson, (String) bodyJson.get("objectType"));
           if (key == null || key.isEmpty()) {
               throw new ObjectNotFoundException(objectNotFoundMessage);
           }

           //Get saved obj on based of key
           plan = this.jsonService.getPlan(key);
           if (plan == null || plan.isEmpty()) {
               throw new ObjectNotFoundException(objectNotFoundMessage);
           } 
           
           
          

           ETag = MD5Utils.hashString(plan.toString());
           String ETagKey = getETagKey(objectType, objectId);
           cache.set(ETagKey, ETag);
           
           

       } catch (JSONException ex) {
           return badRequest(ex.getMessage());
       } catch (Exception e) {
           return internalServerError(e.getMessage());
       }
       return ok(plan.toString(), ETag);

   }
    

    /**
     *
     * @param objectType
     * @param objectId
     * @return
     */
    @RequestMapping(value = "/{object}/{id}", method = RequestMethod.DELETE, produces = "application/json")
    @ResponseBody
    public ResponseEntity deleteJson(@PathVariable("object") String objectType,
                                     @PathVariable("id") String objectId) {
        try {
            if (!this.jsonService.deletePlan(getKey(objectType, objectId))) {
                
                throw new ObjectNotFoundException("Done");
            }
            cache.del(getETagKey(objectType, objectId));
        } catch (ObjectNotFoundException ex) {
            return notFound(ex.getMessage());
        } catch (Exception e) {
            return internalServerError(e.getMessage());
        }
        return ok("{ 'Message' : 'Object Deleted!'}");
    }

    private JSONObject validateSchema(String json) throws JSONException, ValidationException {

        InputStream schemaStream = null;

        JSONObject jsonSchema = null;

        try {
            String value = cache.get("schema");
            if (value == null) {
                throw new ObjectNotFoundException(objectNotFoundMessage);
            }
            jsonSchema = new JSONObject(
                    new JSONTokener(value)
            );
        } catch(ObjectNotFoundException ex){
            schemaStream = RestController.class.getResourceAsStream("/schema.json");
            jsonSchema = new JSONObject(
                    new JSONTokener(schemaStream)
            );
        } catch(Exception e){
            System.out.println("Error while validating");
        }

        JSONObject jsonSubject = new JSONObject(
                new JSONTokener(json)
        );

        Schema schema = SchemaLoader.load(jsonSchema);
        schema.validate(jsonSubject);

        return jsonSubject;

    }

    private JSONObject findInCache(String objectType, String objectId) throws ObjectNotFoundException, JSONException {

        JSONObject res = null;
        String key = getKey(objectType, objectId);
        String value = cache.get(key);
        if (value == null) {
            throw new ObjectNotFoundException(objectNotFoundMessage);
        }
        res = new JSONObject(
                new JSONTokener(value)
        );
        return res;
    }

    private String getKey(String objectType, String objectId) {

        return objectType + "_" + objectId;

    }

    private String getETagKey(String objectType, String objectId) {

        return getKey(objectType, objectId)  + "|" + "ETag";

    }

    private String getEtagString(String objectType, String objectId) {
        return cache.get(getETagKey(objectType, objectId));
    }

    @RequestMapping(value = "/clean-redis", method = RequestMethod.DELETE)
    @ResponseBody
    public ResponseEntity cleanRedis() {

        try {
            Set<String> keys = cache.keys("*");
            cache.del(keys.toArray(new String[keys.size()]));
        } catch (Exception e) {
            return internalServerError(e.getLocalizedMessage());
        }

        return ok("{ message : '" + "All objects deleted!" + "'}");

    }

    @RequestMapping(value = "/schema", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<String> saveSchema(@RequestBody String body,
                                            @RequestHeader Map<String, String> headers) {
        try {
            String key = "schema";
            cache.set(key, body);
        } catch (JSONException | ValidationException e) {
            return badRequest(e.getMessage());
        } catch (Exception e) {
            return internalServerError(e.getMessage());
        }

        return created(cache.get("schema"));

    }


}
