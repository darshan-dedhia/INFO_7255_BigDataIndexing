package edu.info7225.darshandedhia.bigdataindexing.api;

import edu.info7225.darshandedhia.bigdataindexing.Exceptions.ObjectNotFoundException;
import edu.info7225.darshandedhia.bigdataindexing.utils.MD5Utils;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;

import java.io.InputStream;
import java.util.Map;
import java.util.Set;

@Controller
public class RestController extends API{

    /**
     *
     */
    private Jedis cache = new Jedis();

    /**
     *
     *
     * @param body
     * @param headers
     * @return
     */
    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<String> save(@RequestBody String body,
                                       @RequestHeader Map<String, String> headers) {
        String ETag = null;
        try {
            JSONObject jsonObject = validateSchema(body);
            String objType = jsonObject.getString("objectType");
            String objID = jsonObject.getString("objectId");
            String key = getKey(objType, objID);
            cache.set(key, body);

            ETag = MD5Utils.hashString(body);
            String ETagKey = getETagKey(objType, objID);
            cache.set(ETagKey, ETag);

        } catch (JSONException | ValidationException e) {
            return badRequest(e.getMessage());
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
        try {

            String hashString = getEtagString(objectType, objectId);

            if (hashString.equals(ifMatch)) {
                JSONObject jsonObject = findInCache(objectType, objectId);
                return ok(jsonObject.toString(), hashString);
            } else {
                return notModified(null, hashString);
            }

        } catch (ObjectNotFoundException ex) {
            return notFound(ex.getMessage());
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
    @RequestMapping(value = "/{object}/{id}", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity getJson(@PathVariable("object") String objectType,
                                  @PathVariable("id") String objectId) {
        String ETag = null;
        JSONObject jsonObject = null;
        try {
            jsonObject = findInCache(objectType, objectId);

            ETag = getEtagString(objectType, objectId);

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
        try {

            String hashString = getEtagString(objectType, objectId);

            if (!hashString.equals(ifNoneMatch)) {
                JSONObject jsonObject = findInCache(objectType, objectId);
                return ok(jsonObject.toString(), hashString);
            } else {
                return notModified(null, hashString);
            }

        } catch (ObjectNotFoundException ex) {
            return notFound(ex.getMessage());
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
    @RequestMapping(value = "/{object}/{id}", method = RequestMethod.DELETE)
    @ResponseBody
    public ResponseEntity deleteJson(@PathVariable("object") String objectType,
                                     @PathVariable("id") String objectId) {
        try {
            JSONObject jsonObject = findInCache(objectType, objectId);
            cache.del(getKey(objectType, objectId));
            cache.del(getETagKey(objectType, objectId));
        } catch (ObjectNotFoundException ex) {
            return notFound(ex.getMessage());
        } catch (Exception e) {
            return internalServerError(e.getMessage());
        }
        return ok("{ message : 'Object Deleted!'}");
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

    private JSONObject findInCache(String objectType, String objectId) throws ObjectNotFoundException {

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

        return objectType + "|" + objectId;

    }

    private String getETagKey(String objectType, String objectId) {

        return objectType + "|" + objectId + "|" + "ETag";

    }

    private String getEtagString(String objectType, String objectId) {
        return cache.get(getETagKey(objectType, objectId));
    }

    @RequestMapping(value = "/clean-redis", method = RequestMethod.PUT)
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

        return created(createdMessage);

    }


}
