// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.document;

import com.yahoo.document.datatypes.FieldValue;
import com.yahoo.document.fieldpathupdate.FieldPathUpdate;
import com.yahoo.document.serialization.DocumentSerializerFactory;
import com.yahoo.document.serialization.DocumentUpdateReader;
import com.yahoo.document.serialization.DocumentUpdateWriter;
import com.yahoo.document.update.AssignValueUpdate;
import com.yahoo.document.update.ClearValueUpdate;
import com.yahoo.document.update.FieldUpdate;
import com.yahoo.document.update.ValueUpdate;
import com.yahoo.io.GrowableByteBuffer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * <p>Specifies one or more field updates to a document.</p> <p>A document update contains a list of {@link
 * com.yahoo.document.update.FieldUpdate field updates} for fields to be updated by this update. Each field update is
 * applied atomically, but the entire document update is not. A document update can only contain one field update per
 * field. To make multiple updates to the same field in the same document update, add multiple {@link
 * com.yahoo.document.update.ValueUpdate value updates} to the same field update.</p> <p>To update a document and
 * set a string field to a new value:</p>
 * <pre>
 * DocumentType musicType = DocumentTypeManager.getInstance().getDocumentType("music", 0);
 * DocumentUpdate docUpdate = new DocumentUpdate(musicType,
 *         new DocumentId("doc:test:http://music.yahoo.com/"));
 * FieldUpdate update = FieldUpdate.createAssign(musicType.getField("artist"), "lillbabs");
 * docUpdate.addFieldUpdate(update);
 * </pre>
 *
 * @author Einar M R Rosenvinge
 * @see com.yahoo.document.update.FieldUpdate
 * @see com.yahoo.document.update.ValueUpdate
 */
//TODO Vespa 7 Remove all deprecated methods

public class DocumentUpdate extends DocumentOperation implements Iterable<FieldPathUpdate> {

    //see src/vespa/document/util/identifiableid.h
    public static final int CLASSID = 0x1000 + 6;

    private DocumentId docId;
    private List<FieldUpdate> fieldUpdates;
    private final Map<Integer, FieldUpdate> id2FieldUpdateMap;
    private List<FieldPathUpdate> fieldPathUpdates;
    private DocumentType documentType;
    private Optional<Boolean> createIfNonExistent = Optional.empty();

    /**
     * Creates a DocumentUpdate.
     *
     * @param docId   the ID of the update
     * @param docType the document type that this update is valid for
     */
    public DocumentUpdate(DocumentType docType, DocumentId docId) {
        this(docType, docId, new HashMap<>());
    }

    /**
     * Creates a new document update using a reader
     */
    public DocumentUpdate(DocumentUpdateReader reader) {
        docId = null;
        documentType = null;
        fieldUpdates = new ArrayList<>();
        id2FieldUpdateMap = new HashMap<>();
        fieldPathUpdates = new ArrayList<>();
        reader.read(this);
    }

    /**
     * Creates a DocumentUpdate.
     *
     * @param docId   the ID of the update
     * @param docType the document type that this update is valid for
     */
    public DocumentUpdate(DocumentType docType, String docId) {
        this(docType, new DocumentId(docId));
    }

    private DocumentUpdate(DocumentType docType, DocumentId docId, Map<Integer, FieldUpdate> id2fieldUpdateMap) {
        this.docId = docId;
        this.documentType = docType;
        this.fieldUpdates = new ArrayList<>(id2fieldUpdateMap.values());
        id2FieldUpdateMap = id2fieldUpdateMap;
        this.fieldPathUpdates = new ArrayList<>();
    }

    public DocumentId getId() {
        return docId;
    }

    /**
     * Sets the document id of the document to update.
     * Use only while deserializing - changing the document id after creation has undefined behaviour.
     */
    public void setId(DocumentId id) {
        docId = id;
    }

    private void verifyType(Document doc) {
        if (!documentType.equals(doc.getDataType())) {
            throw new IllegalArgumentException(
                    "Document " + doc.getId() + " with type " + doc.getDataType() + " must have same type as update, which is type " + documentType);
        }
    }
    /**
     * Applies this document update.
     *
     * @param doc the document to apply the update to
     * @return a reference to itself
     * @throws IllegalArgumentException if the document does not have the same document type as this update
     */
    public DocumentUpdate applyTo(Document doc) {
        verifyType(doc);

        for (FieldUpdate fieldUpdate : id2FieldUpdateMap.values()) {
            fieldUpdate.applyTo(doc);
        }
        for (FieldPathUpdate fieldPathUpdate : fieldPathUpdates) {
            fieldPathUpdate.applyTo(doc);
        }
        return this;
    }

    /**
     * Prune away any field update that will not modify any field in the document.
     * @param doc document to check against
     * @return a reference to itself
     * @throws IllegalArgumentException if the document does not have the same document type as this update
     */
    public DocumentUpdate prune(Document doc) {
        verifyType(doc);

        for (Iterator<Map.Entry<Integer, FieldUpdate>> iter = id2FieldUpdateMap.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<Integer, FieldUpdate> entry = iter.next();
            FieldUpdate update = entry.getValue();
            if (!update.isEmpty()) {
                ValueUpdate last = update.getValueUpdate(update.size() - 1);
                if (last instanceof AssignValueUpdate) {
                    FieldValue currentValue = doc.getFieldValue(update.getField());
                    if ((currentValue != null) && currentValue.equals(last.getValue())) {
                        iter.remove();
                    }
                } else if (last instanceof ClearValueUpdate) {
                    FieldValue currentValue = doc.getFieldValue(update.getField());
                    if (currentValue == null) {
                        iter.remove();
                    } else {
                        FieldValue copy = currentValue.clone();
                        copy.clear();
                        if (currentValue.equals(copy)) {
                            iter.remove();
                        }
                    }
                }
            }
        }
        return this;
    }

    /**
     * Get an unmodifiable list of all field updates that this document update specifies.
     *
     * @return a list of all FieldUpdates in this DocumentUpdate
     * @deprecated Use fieldUpdates() instead.
     */
    @Deprecated
    public List<FieldUpdate> getFieldUpdates() {
        return Collections.unmodifiableList(fieldUpdates);
    }

    /**
     * Get an unmodifiable collection of all field updates that this document update specifies.
     *
     * @return a collection of all FieldUpdates in this DocumentUpdate
     */
    public Collection<FieldUpdate> fieldUpdates() {
        return Collections.unmodifiableCollection(id2FieldUpdateMap.values());
    }

    /**
     * Get an unmodifiable list of all field path updates this document update specifies.
     *
     * @return Returns a list of all field path updates in this document update.
     * @deprecated Use fieldPathUpdates() instead.
     */
    @Deprecated
    public List<FieldPathUpdate> getFieldPathUpdates() {
        return Collections.unmodifiableList(fieldPathUpdates);
    }

    /**
     * Get an unmodifiable collection of all field path updates that this document update specifies.
     *
     * @return a collection of all FieldPathUpdates in this DocumentUpdate
     */
    public Collection<FieldPathUpdate> fieldPathUpdates() {
        return Collections.unmodifiableCollection(fieldPathUpdates);
    }

    /** Returns the type of the document this updates
     *
     * @return The documentype of the document
     */
    public DocumentType getDocumentType() {
        return documentType;
    }

    /**
     * Sets the document type. Use only while deserializing - changing the document type after creation
     * has undefined behaviour.
     */
    public void setDocumentType(DocumentType type) {
        documentType = type;
    }

    /**
     * Get the field update at the specified index in the list of field updates.
     *
     * @param index the index of the FieldUpdate to return
     * @return the FieldUpdate at the specified index
     * @throws IndexOutOfBoundsException if index is out of range
     * @deprecated use getFieldUpdate(Field field) instead.
     */
    @Deprecated
    public FieldUpdate getFieldUpdate(int index) {
        return fieldUpdates.get(index);
    }

    /**
     * Replaces the field update at the specified index in the list of field updates.
     *
     * @param index index of the FieldUpdate to replace
     * @param upd   the FieldUpdate to be stored at the specified position
     * @return the FieldUpdate previously at the specified position
     * @throws IndexOutOfBoundsException if index is out of range
     * @deprecated Use removeFieldUpdate/addFieldUpdate instead
     */
    @Deprecated
    public FieldUpdate setFieldUpdate(int index, FieldUpdate upd) {
        FieldUpdate old = fieldUpdates.get(index);
        if (old.getField().equals(upd.getField())) {
            fieldUpdates.set(index, upd);
            id2FieldUpdateMap.put(upd.getField().getId(), upd);
        } else {
            throw new IllegalArgumentException("You can not replace a FieldUpdate for field '" + old.getField() +
                                               "' with an update for field '" + upd.getField() + "'");
        }

        return old;
    }

    /**
     * Returns the update for a field
     *
     * @param field the field to return the update of
     * @return the update for the field, or null if that field has no update in this
     */
    public FieldUpdate getFieldUpdate(Field field) {
        return getFieldUpdateById(field.getId());
    }

    /** Removes all field updates from the list for field updates. */
    public void clearFieldUpdates() {
        fieldUpdates.clear();
        id2FieldUpdateMap.clear();
    }

    /**
     * Returns the update for a field name
     *
     * @param fieldName the field name to return the update of
     * @return the update for the field, or null if that field has no update in this
     */
    public FieldUpdate getFieldUpdate(String fieldName) {
        for (FieldUpdate fieldUpdate : id2FieldUpdateMap.values()) {
            if (fieldUpdate.getField().getName().equals(fieldName))
                return fieldUpdate;
        }
        return null;
    }
    private FieldUpdate getFieldUpdateById(Integer fieldId) {
        return id2FieldUpdateMap.get(fieldId);
    }

    /**
     * Assigns the field updates of this document update.
     * This document update receives ownership of the list - it can not be subsequently used
     * by the caller.
     *
     * @param fieldUpdates the new list of updates of this
     * @throws NullPointerException if the argument passed is null
     * @deprecated Iterate and use addFieldUpdate instead
     */
    @Deprecated
    public void setFieldUpdates(Collection<FieldUpdate> fieldUpdates) {
        if (fieldUpdates == null) {
            throw new NullPointerException("The field updates of a document update can not be null");
        }
        fieldUpdates.clear();
        for (FieldUpdate fieldUpdate : fieldUpdates) {
            addFieldUpdate(fieldUpdate);
        }
    }

    /**
     * Get the number of field updates in this document update.
     *
     * @return the size of the List of FieldUpdates
     */
    public int size() {
        return id2FieldUpdateMap.size();
    }

    /**
     * Adds the given {@link FieldUpdate} to this DocumentUpdate. If this DocumentUpdate already contains a FieldUpdate
     * for the named field, the content of the given FieldUpdate is added to the existing one.
     *
     * @param update The FieldUpdate to add to this DocumentUpdate.
     * @return This, to allow chaining.
     * @throws IllegalArgumentException If the {@link DocumentType} of this DocumentUpdate does not have a corresponding
     *                                  field.
     */
    public DocumentUpdate addFieldUpdate(FieldUpdate update) {
        Integer fieldId = update.getField().getId();
        if (documentType.getField(fieldId) == null) {
            throw new IllegalArgumentException("Document type '" + documentType.getName() + "' does not have field '" + update.getField().getName() + "'.");
        }
        FieldUpdate prevUpdate = getFieldUpdateById(fieldId);
        if (prevUpdate != update) {
            if (prevUpdate != null) {
                prevUpdate.addAll(update);
            } else {
                fieldUpdates.add(update);
                id2FieldUpdateMap.put(fieldId, update);
            }
        }
        return this;
    }

    /**
     * Adds a field path update to perform on the document.
     *
     * @return a reference to itself.
     */
    public DocumentUpdate addFieldPathUpdate(FieldPathUpdate fieldPathUpdate) {
        fieldPathUpdates.add(fieldPathUpdate);
        return this;
    }

    /**
     * Adds all the field- and field path updates of the given document update to this. If the given update refers to a
     * different document or document type than this, this method throws an exception.
     *
     * @param update The update whose content to add to this.
     * @throws IllegalArgumentException If the {@link DocumentId} or {@link DocumentType} of the given DocumentUpdate
     *                                  does not match the content of this.
     */
    public void addAll(DocumentUpdate update) {
        if (update == null) {
            return;
        }
        if (!docId.equals(update.docId)) {
            throw new IllegalArgumentException("Expected " + docId + ", got " + update.docId + ".");
        }
        if (!documentType.equals(update.documentType)) {
            throw new IllegalArgumentException("Expected " + documentType + ", got " + update.documentType + ".");
        }
        for (FieldUpdate fieldUpd : update.fieldUpdates()) {
            addFieldUpdate(fieldUpd);
        }
        for (FieldPathUpdate pathUpd : update.fieldPathUpdates) {
            addFieldPathUpdate(pathUpd);
        }
    }

    /**
     * Removes the field update at the specified position in the list of field updates.
     *
     * @param index the index of the FieldUpdate to remove
     * @return the FieldUpdate previously at the specified position
     * @throws IndexOutOfBoundsException if index is out of range
     * @deprecated use removeFieldUpdate(Field field) instead.
     */
    @Deprecated
    public FieldUpdate removeFieldUpdate(int index) {
        FieldUpdate prev = getFieldUpdate(index);
        fieldUpdates.remove(index);
        return removeFieldUpdate(prev.getField());
    }

    public FieldUpdate removeFieldUpdate(Field field) {
        return id2FieldUpdateMap.remove(field.getId());
    }

    /**
     * Returns the document type of this document update.
     *
     * @return the document type of this document update
     */
    public DocumentType getType() {
        return documentType;
    }

    public final void serialize(GrowableByteBuffer buf) {
        serialize(DocumentSerializerFactory.create42(buf));
    }

    public void serialize(DocumentUpdateWriter data) {
        data.write(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DocumentUpdate)) return false;

        DocumentUpdate that = (DocumentUpdate) o;

        if (docId != null ? !docId.equals(that.docId) : that.docId != null) return false;
        if (documentType != null ? !documentType.equals(that.documentType) : that.documentType != null) return false;
        if (fieldPathUpdates != null ? !fieldPathUpdates.equals(that.fieldPathUpdates) : that.fieldPathUpdates != null)
            return false;
        if (fieldUpdates != null ? !fieldUpdates.equals(that.fieldUpdates) : that.fieldUpdates != null) return false;
        if (this.getCreateIfNonExistent() != ((DocumentUpdate) o).getCreateIfNonExistent()) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = docId != null ? docId.hashCode() : 0;
        result = 31 * result + (id2FieldUpdateMap != null ? id2FieldUpdateMap.hashCode() : 0);
        result = 31 * result + (fieldPathUpdates != null ? fieldPathUpdates.hashCode() : 0);
        result = 31 * result + (documentType != null ? documentType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder string = new StringBuilder();
        string.append("update of document '");
        string.append(docId);
        string.append("': ");
        string.append("create-if-non-existent=");
        string.append(createIfNonExistent.orElse(false));
        string.append(": ");
        string.append("[");

        for (Iterator<FieldUpdate> i = id2FieldUpdateMap.values().iterator(); i.hasNext();) {
            FieldUpdate fieldUpdate = i.next();
            string.append(fieldUpdate);
            if (i.hasNext()) {
                string.append(", ");
            }
        }
        string.append("]");

        if (fieldPathUpdates.size() > 0) {
            string.append(" [ ");
            for (FieldPathUpdate up : fieldPathUpdates) {
                string.append(up.toString() + " ");
            }
            string.append(" ]");
        }

        return string.toString();
    }

    public Iterator<FieldPathUpdate> iterator() {
        return fieldPathUpdates.iterator();
    }

    /**
     * Returns whether or not this field update contains any field- or field path updates.
     *
     * @return True if this update is empty.
     */
    public boolean isEmpty() {
        return id2FieldUpdateMap.isEmpty() && fieldPathUpdates.isEmpty();
    }

    /**
     * Sets whether this update should create the document it updates if that document does not exist.
     * In this case an empty document is created before the update is applied.
     *
     * @since 5.17
     * @param value Whether the document it updates should be created.
     */
    public void setCreateIfNonExistent(boolean value) {
        createIfNonExistent = Optional.of(value);
    }

    /**
     * Gets whether this update should create the document it updates if that document does not exist.
     *
     * @since 5.17
     * @return Whether the document it updates should be created.
     */
    public boolean getCreateIfNonExistent() {
        return createIfNonExistent.orElse(false);
    }

    public Optional<Boolean> getOptionalCreateIfNonExistent() {
        return createIfNonExistent;
    }
}
