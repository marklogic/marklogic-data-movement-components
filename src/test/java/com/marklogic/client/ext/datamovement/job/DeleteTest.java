package com.marklogic.client.ext.datamovement.job;

import com.marklogic.client.ext.datamovement.AbstractDataMovementTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class DeleteTest extends AbstractDataMovementTest {

    @Test
    public void deleteBothDocs() {
        DeleteJob job = new DeleteJob();
        job.setWhereUris(FIRST_URI, SECOND_URI);

        assertNotNull(client.newDocumentManager().exists(FIRST_URI));
        assertNotNull(client.newDocumentManager().exists(SECOND_URI));

        job.run(client);

        assertNull(client.newDocumentManager().exists(FIRST_URI));
        assertNull(client.newDocumentManager().exists(SECOND_URI));
    }

    @Test
    public void deleteOneDoc() {
        DeleteJob job = new DeleteJob();
        job.setWhereUris(FIRST_URI);

        assertNotNull(client.newDocumentManager().exists(FIRST_URI));
        assertNotNull(client.newDocumentManager().exists(SECOND_URI));

        job.run(client);

        assertNull(client.newDocumentManager().exists(FIRST_URI));
        assertNotNull(client.newDocumentManager().exists(SECOND_URI));
    }
}
